using System.Net.WebSockets;
using System.Text.Json;
using StateBroker.Core;
using StateBroker.Transport;

namespace StateBroker.Tests;

public class BrokerIntegrationTests : IAsyncLifetime
{
    private readonly int _port;
    private readonly CancellationTokenSource _cts = new();
    private Broker _broker = null!;
    private Task _brokerTask = null!;

    public BrokerIntegrationTests()
    {
        var listener = new System.Net.Sockets.TcpListener(
            System.Net.IPAddress.Loopback, 0);
        listener.Start();
        _port = ((System.Net.IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
    }

    public Task InitializeAsync()
    {
        var prefix = $"http://localhost:{_port}/";
        var transport = new WebSocketTransport(prefix);
        var store = new ShardedStateStore();
        var consensus = new NoopConsensus();
        var sessions = new SessionManager();
        var router = new TopicRouter();
        // Fast retry for tests: 100ms base, 500ms max
        var qos = new QosEngine(TimeSpan.FromMilliseconds(100), TimeSpan.FromMilliseconds(500));

        _broker = new Broker(transport, store, consensus, sessions, router, qos);
        _brokerTask = _broker.RunAsync(_cts.Token);

        return Task.Delay(200);
    }

    public async Task DisposeAsync()
    {
        _cts.Cancel();
        try { await _brokerTask; }
        catch (OperationCanceledException) { }
        _cts.Dispose();
    }

    private async Task<ClientWebSocket> ConnectAsync(string clientId)
    {
        var ws = new ClientWebSocket();
        var uri = new Uri($"ws://localhost:{_port}/?clientId={clientId}");
        await ws.ConnectAsync(uri, CancellationToken.None);
        return ws;
    }

    private static async Task SendFrameAsync(ClientWebSocket ws, Frame frame)
    {
        var bytes = JsonSerializer.SerializeToUtf8Bytes(frame, BrokerJsonContext.Default.Frame);
        await ws.SendAsync(
            new ArraySegment<byte>(bytes),
            WebSocketMessageType.Text,
            endOfMessage: true,
            CancellationToken.None);
    }

    private static async Task<Frame?> ReceiveFrameAsync(
        ClientWebSocket ws, TimeSpan? timeout = null)
    {
        using var cts = new CancellationTokenSource(timeout ?? TimeSpan.FromSeconds(2));
        var buffer = new byte[4096];
        using var ms = new MemoryStream();

        WebSocketReceiveResult result;
        do
        {
            result = await ws.ReceiveAsync(new ArraySegment<byte>(buffer), cts.Token);
            ms.Write(buffer, 0, result.Count);
        } while (!result.EndOfMessage);

        if (result.MessageType == WebSocketMessageType.Close)
            return null;

        return JsonSerializer.Deserialize(ms.ToArray(), BrokerJsonContext.Default.Frame);
    }

    private static async Task<List<Frame>> ReceiveFramesAsync(
        ClientWebSocket ws, int count, TimeSpan? timeout = null)
    {
        var frames = new List<Frame>();
        for (var i = 0; i < count; i++)
        {
            var f = await ReceiveFrameAsync(ws, timeout);
            if (f is not null) frames.Add(f);
        }
        return frames;
    }

    /// <summary>Send SUBSCRIBE and drain the SUBACK response.</summary>
    private static async Task SubscribeAsync(ClientWebSocket ws, string topic)
    {
        await SendFrameAsync(ws, new Frame(Frame.Types.Subscribe, topic));
        var suback = await ReceiveFrameAsync(ws);
        Assert.Equal(Frame.Types.SubAck, suback!.Type);
    }

    // ── Connection ──

    [Fact]
    public async Task Client_can_connect()
    {
        using var ws = await ConnectAsync("test-1");
        Assert.Equal(WebSocketState.Open, ws.State);
    }

    [Fact]
    public async Task Multiple_clients_connect()
    {
        using var ws1 = await ConnectAsync("c1");
        using var ws2 = await ConnectAsync("c2");

        Assert.Equal(WebSocketState.Open, ws1.State);
        Assert.Equal(WebSocketState.Open, ws2.State);

        await Task.Delay(100);
        Assert.True(_broker.Sessions.Count >= 2);
    }

    // ── PING / PONG ──

    [Fact]
    public async Task Ping_returns_pong()
    {
        using var ws = await ConnectAsync("ping-test");
        await SendFrameAsync(ws, new Frame(Frame.Types.Ping));

        var response = await ReceiveFrameAsync(ws);
        Assert.NotNull(response);
        Assert.Equal(Frame.Types.Pong, response!.Type);
    }

    [Fact]
    public async Task Multiple_pings()
    {
        using var ws = await ConnectAsync("multi-ping");

        for (var i = 0; i < 5; i++)
        {
            await SendFrameAsync(ws, new Frame(Frame.Types.Ping));
            var response = await ReceiveFrameAsync(ws);
            Assert.Equal(Frame.Types.Pong, response!.Type);
        }
    }

    // ── PUBLISH retain ──

    [Fact]
    public async Task Publish_retain_stores_state()
    {
        using var ws = await ConnectAsync("pub-test");

        var payload = JsonDocument.Parse("""{"temp":22.5}""").RootElement;
        await SendFrameAsync(ws, new Frame(
            Frame.Types.Publish, "sensors/temp", Retain: true, Payload: payload));

        await Task.Delay(100);

        var stored = _broker.Store.Get("sensors/temp");
        Assert.NotNull(stored);
        Assert.Equal(22.5, stored!.Value.GetProperty("temp").GetDouble());
    }

    [Fact]
    public async Task Publish_retain_overwrites()
    {
        using var ws = await ConnectAsync("pub-overwrite");

        var p1 = JsonDocument.Parse("1").RootElement;
        var p2 = JsonDocument.Parse("2").RootElement;

        await SendFrameAsync(ws, new Frame(
            Frame.Types.Publish, "counter", Retain: true, Payload: p1));
        await Task.Delay(50);
        await SendFrameAsync(ws, new Frame(
            Frame.Types.Publish, "counter", Retain: true, Payload: p2));
        await Task.Delay(100);

        Assert.Equal(2, _broker.Store.Get("counter")!.Value.GetInt32());
    }

    [Fact]
    public async Task Publish_non_retain_does_not_store()
    {
        using var ws = await ConnectAsync("pub-ephemeral");

        var payload = JsonDocument.Parse("99").RootElement;
        await SendFrameAsync(ws, new Frame(
            Frame.Types.Publish, "ephemeral/topic", Retain: false, Payload: payload));

        await Task.Delay(100);
        Assert.Null(_broker.Store.Get("ephemeral/topic"));
    }

    // ── Session persistence ──

    [Fact]
    public async Task Session_created_on_connect()
    {
        using var ws = await ConnectAsync("session-test");
        await Task.Delay(100);

        Assert.True(_broker.Sessions.TryGet("session-test", out var session));
        Assert.NotNull(session);
    }

    // ── Graceful close ──

    [Fact]
    public async Task Client_close_is_handled()
    {
        var ws = await ConnectAsync("close-test");
        await ws.CloseAsync(
            WebSocketCloseStatus.NormalClosure, null, CancellationToken.None);

        using var ws2 = await ConnectAsync("after-close");
        Assert.Equal(WebSocketState.Open, ws2.State);
    }

    // ── SUBSCRIBE + PUBLISH fan-out ──

    [Fact]
    public async Task Subscribe_then_publish_delivers()
    {
        using var sub = await ConnectAsync("sub-1");
        using var pub = await ConnectAsync("pub-1");

        // Subscribe
        await SubscribeAsync(sub, "events/+");
        await Task.Delay(100);

        // Publish (QoS 0)
        var payload = JsonDocument.Parse("""{"v":42}""").RootElement;
        await SendFrameAsync(pub, new Frame(
            Frame.Types.Publish, "events/click", Payload: payload));

        // Subscriber should receive DELIVER
        var deliver = await ReceiveFrameAsync(sub);
        Assert.NotNull(deliver);
        Assert.Equal(Frame.Types.Deliver, deliver!.Type);
        Assert.Equal("events/click", deliver.Topic);
        Assert.Equal(42, deliver.Payload.GetProperty("v").GetInt32());
    }

    [Fact]
    public async Task Subscribe_receives_retained_state()
    {
        using var pub = await ConnectAsync("pub-retain");

        // Publish retained state first
        var payload = JsonDocument.Parse("100").RootElement;
        await SendFrameAsync(pub, new Frame(
            Frame.Types.Publish, "state/counter", Retain: true, Payload: payload));
        await Task.Delay(100);

        // New subscriber should receive retained state on subscribe
        using var sub = await ConnectAsync("sub-retain");
        await SubscribeAsync(sub, "state/#");

        var deliver = await ReceiveFrameAsync(sub);
        Assert.NotNull(deliver);
        Assert.Equal(Frame.Types.Deliver, deliver!.Type);
        Assert.Equal("state/counter", deliver.Topic);
        Assert.True(deliver.Retained);
        Assert.Equal(1, deliver.Qos);
        Assert.NotNull(deliver.MsgId);
        Assert.Equal(100, deliver.Payload.GetInt32());

        // Retained push is in the QoS outbox (QoS 1, requires ACK)
        var pending = _broker.Qos.GetPending("sub-retain");
        Assert.Single(pending);
        Assert.Equal(deliver.MsgId, pending[0].MsgId);

        // ACK clears outbox
        await SendFrameAsync(sub, new Frame(Frame.Types.Ack, MsgId: deliver.MsgId));
        await Task.Delay(100);
        Assert.Empty(_broker.Qos.GetPending("sub-retain"));
    }

    [Fact]
    public async Task Publish_qos1_creates_outbox_entry()
    {
        using var sub = await ConnectAsync("sub-qos1");
        using var pub = await ConnectAsync("pub-qos1");

        await SubscribeAsync(sub, "qos/test");
        await Task.Delay(100);

        // Publish QoS 1
        var payload = JsonDocument.Parse("1").RootElement;
        await SendFrameAsync(pub, new Frame(
            Frame.Types.Publish, "qos/test", Qos: 1, Payload: payload));

        // Subscriber receives DELIVER
        var deliver = await ReceiveFrameAsync(sub);
        Assert.NotNull(deliver);
        Assert.Equal(Frame.Types.Deliver, deliver!.Type);
        Assert.NotNull(deliver.MsgId);
        Assert.Equal(1, deliver.Qos);

        // QoS engine should have pending entry
        await Task.Delay(50);
        var pending = _broker.Qos.GetPending("sub-qos1");
        Assert.True(pending.Count >= 1);
    }

    [Fact]
    public async Task Ack_removes_from_outbox()
    {
        using var sub = await ConnectAsync("sub-ack");
        using var pub = await ConnectAsync("pub-ack");

        await SubscribeAsync(sub, "ack/test");
        await Task.Delay(100);

        var payload = JsonDocument.Parse("1").RootElement;
        await SendFrameAsync(pub, new Frame(
            Frame.Types.Publish, "ack/test", Qos: 1, Payload: payload));

        var deliver = await ReceiveFrameAsync(sub);
        Assert.NotNull(deliver?.MsgId);

        // Send ACK
        await SendFrameAsync(sub, new Frame(Frame.Types.Ack, MsgId: deliver!.MsgId));
        await Task.Delay(100);

        // Outbox should be empty
        var pending = _broker.Qos.GetPending("sub-ack");
        Assert.Empty(pending);
    }

    [Fact]
    public async Task Multiple_subscribers_all_receive()
    {
        using var sub1 = await ConnectAsync("multi-sub-1");
        using var sub2 = await ConnectAsync("multi-sub-2");
        using var pub = await ConnectAsync("multi-pub");

        await SubscribeAsync(sub1, "fan/out");
        await SubscribeAsync(sub2, "fan/out");
        await Task.Delay(100);

        var payload = JsonDocument.Parse("""{"msg":"hello"}""").RootElement;
        await SendFrameAsync(pub, new Frame(
            Frame.Types.Publish, "fan/out", Payload: payload));

        var d1 = await ReceiveFrameAsync(sub1);
        var d2 = await ReceiveFrameAsync(sub2);

        Assert.Equal(Frame.Types.Deliver, d1!.Type);
        Assert.Equal(Frame.Types.Deliver, d2!.Type);
        Assert.Equal("fan/out", d1.Topic);
        Assert.Equal("fan/out", d2.Topic);
    }

    [Fact]
    public async Task Wildcard_subscription_receives_matching()
    {
        using var sub = await ConnectAsync("wild-sub");
        using var pub = await ConnectAsync("wild-pub");

        await SubscribeAsync(sub, "sensors/#");
        await Task.Delay(100);

        var payload = JsonDocument.Parse("25").RootElement;
        await SendFrameAsync(pub, new Frame(
            Frame.Types.Publish, "sensors/temp/living", Payload: payload));

        var deliver = await ReceiveFrameAsync(sub);
        Assert.Equal(Frame.Types.Deliver, deliver!.Type);
        Assert.Equal("sensors/temp/living", deliver.Topic);
    }

    // ── QoS 1 retry ──

    [Fact]
    public async Task Unacked_qos1_is_retried()
    {
        using var sub = await ConnectAsync("sub-retry");
        using var pub = await ConnectAsync("pub-retry");

        await SubscribeAsync(sub, "retry/test");
        await Task.Delay(100);

        // Publish QoS 1
        var payload = JsonDocument.Parse("1").RootElement;
        await SendFrameAsync(pub, new Frame(
            Frame.Types.Publish, "retry/test", Qos: 1, Payload: payload));

        // Receive first delivery — don't ACK
        var first = await ReceiveFrameAsync(sub);
        Assert.NotNull(first);
        Assert.Equal(Frame.Types.Deliver, first!.Type);

        // Wait for retry (100ms base in test config)
        var retry = await ReceiveFrameAsync(sub, TimeSpan.FromSeconds(2));
        Assert.NotNull(retry);
        Assert.Equal(Frame.Types.Deliver, retry!.Type);
        Assert.Equal(first.MsgId, retry.MsgId); // same msgId = same frame retried

        // Now ACK — retries should stop
        await SendFrameAsync(sub, new Frame(Frame.Types.Ack, MsgId: first.MsgId));
        await Task.Delay(300); // wait past retry interval
        Assert.Empty(_broker.Qos.GetPending("sub-retry"));
    }

    [Fact]
    public async Task Retained_state_not_duplicated_on_reconnect()
    {
        using var pub = await ConnectAsync("pub-recon");

        // Publish retained state
        var payload = JsonDocument.Parse("42").RootElement;
        await SendFrameAsync(pub, new Frame(
            Frame.Types.Publish, "recon/topic", Retain: true, Payload: payload));
        await Task.Delay(100);

        // First connection — subscribe, receive retained (now QoS 1)
        var ws1 = await ConnectAsync("sub-recon");
        await SubscribeAsync(ws1, "recon/#");
        var d1 = await ReceiveFrameAsync(ws1);
        Assert.NotNull(d1);
        Assert.True(d1!.Retained);
        Assert.NotNull(d1.MsgId);

        // Disconnect without ACKing — outbox has the retained push
        await ws1.CloseAsync(
            System.Net.WebSockets.WebSocketCloseStatus.NormalClosure, null, CancellationToken.None);
        await Task.Delay(200);

        // Second connection — same clientId, resubscribe
        // EnqueueAsync replaces the stale outbox entry with a fresh retained push
        using var ws2 = await ConnectAsync("sub-recon");
        await SubscribeAsync(ws2, "recon/#");

        // Should receive exactly ONE retained delivery (new MsgId from fresh push)
        var d2 = await ReceiveFrameAsync(ws2);
        Assert.NotNull(d2);
        Assert.Equal("recon/topic", d2!.Topic);
        Assert.True(d2.Retained);
        Assert.Equal(42, d2.Payload.GetInt32());
        Assert.NotNull(d2.MsgId);

        // ACK it
        await SendFrameAsync(ws2, new Frame(Frame.Types.Ack, MsgId: d2.MsgId));
        await Task.Delay(100);
        Assert.Empty(_broker.Qos.GetPending("sub-recon"));

        // No duplicate frame should arrive
        try
        {
            var extra = await ReceiveFrameAsync(ws2, TimeSpan.FromMilliseconds(500));
            Assert.True(extra is null || extra.Topic != "recon/topic",
                "Should not receive duplicate retained delivery");
        }
        catch (OperationCanceledException) { }
    }

    // ── Unacked scenarios ──

    [Fact]
    public async Task Unacked_newer_publish_replaces_older_in_outbox()
    {
        using var sub = await ConnectAsync("sub-replace");
        using var pub = await ConnectAsync("pub-replace");

        await SubscribeAsync(sub, "state/val");
        await Task.Delay(100);

        // Publish 222 then 333 to same topic — don't ACK either
        var p1 = JsonDocument.Parse("222").RootElement;
        await SendFrameAsync(pub, new Frame(
            Frame.Types.Publish, "state/val", Qos: 1, Payload: p1));
        var d1 = await ReceiveFrameAsync(sub);
        Assert.Equal(222, d1!.Payload.GetInt32());

        var p2 = JsonDocument.Parse("333").RootElement;
        await SendFrameAsync(pub, new Frame(
            Frame.Types.Publish, "state/val", Qos: 1, Payload: p2));
        var d2 = await ReceiveFrameAsync(sub);
        Assert.Equal(333, d2!.Payload.GetInt32());

        // Outbox should have only ONE entry — the latest (333)
        await Task.Delay(50);
        var pending = _broker.Qos.GetPending("sub-replace");
        Assert.Single(pending);
        Assert.Equal(333, pending[0].Payload.GetInt32());
        Assert.Equal(d2.MsgId, pending[0].MsgId);
    }

    [Fact]
    public async Task Unacked_retries_only_latest_value()
    {
        using var sub = await ConnectAsync("sub-latest");
        using var pub = await ConnectAsync("pub-latest");

        await SubscribeAsync(sub, "latest/test");
        await Task.Delay(100);

        // Publish two values to same topic without ACKing
        await SendFrameAsync(pub, new Frame(
            Frame.Types.Publish, "latest/test", Qos: 1,
            Payload: JsonDocument.Parse("111").RootElement));
        var first = await ReceiveFrameAsync(sub);

        await SendFrameAsync(pub, new Frame(
            Frame.Types.Publish, "latest/test", Qos: 1,
            Payload: JsonDocument.Parse("222").RootElement));
        var second = await ReceiveFrameAsync(sub);

        // Wait for retry — should only get 222, never 111
        var retry = await ReceiveFrameAsync(sub, TimeSpan.FromSeconds(2));
        Assert.NotNull(retry);
        Assert.Equal(222, retry!.Payload.GetInt32());
        Assert.Equal(second!.MsgId, retry.MsgId);
    }

    [Fact]
    public async Task Ack_stale_msgId_after_replace_is_noop()
    {
        using var sub = await ConnectAsync("sub-stale");
        using var pub = await ConnectAsync("pub-stale");

        await SubscribeAsync(sub, "stale/test");
        await Task.Delay(100);

        // Publish v1
        await SendFrameAsync(pub, new Frame(
            Frame.Types.Publish, "stale/test", Qos: 1,
            Payload: JsonDocument.Parse("1").RootElement));
        var d1 = await ReceiveFrameAsync(sub);

        // Publish v2 — replaces v1 in outbox
        await SendFrameAsync(pub, new Frame(
            Frame.Types.Publish, "stale/test", Qos: 1,
            Payload: JsonDocument.Parse("2").RootElement));
        var d2 = await ReceiveFrameAsync(sub);

        // ACK the stale v1 msgId — should be no-op, v2 still pending
        await SendFrameAsync(sub, new Frame(Frame.Types.Ack, MsgId: d1!.MsgId));
        await Task.Delay(100);

        var pending = _broker.Qos.GetPending("sub-stale");
        Assert.Single(pending);
        Assert.Equal(d2!.MsgId, pending[0].MsgId);

        // ACK the current v2 — outbox empty
        await SendFrameAsync(sub, new Frame(Frame.Types.Ack, MsgId: d2.MsgId));
        await Task.Delay(100);
        Assert.Empty(_broker.Qos.GetPending("sub-stale"));
    }

    [Fact]
    public async Task Unacked_multiple_topics_each_retains_latest()
    {
        using var sub = await ConnectAsync("sub-multi-topic");
        using var pub = await ConnectAsync("pub-multi-topic");

        await SubscribeAsync(sub, "mt/#");
        await Task.Delay(100);

        // Publish to two different topics
        await SendFrameAsync(pub, new Frame(
            Frame.Types.Publish, "mt/a", Qos: 1,
            Payload: JsonDocument.Parse("10").RootElement));
        await ReceiveFrameAsync(sub);

        await SendFrameAsync(pub, new Frame(
            Frame.Types.Publish, "mt/b", Qos: 1,
            Payload: JsonDocument.Parse("20").RootElement));
        await ReceiveFrameAsync(sub);

        // Update mt/a — should replace, mt/b stays
        await SendFrameAsync(pub, new Frame(
            Frame.Types.Publish, "mt/a", Qos: 1,
            Payload: JsonDocument.Parse("11").RootElement));
        await ReceiveFrameAsync(sub);

        await Task.Delay(50);
        var pending = _broker.Qos.GetPending("sub-multi-topic");
        Assert.Equal(2, pending.Count);

        var byTopic = pending.ToDictionary(f => f.Topic!);
        Assert.Equal(11, byTopic["mt/a"].Payload.GetInt32());
        Assert.Equal(20, byTopic["mt/b"].Payload.GetInt32());
    }

    [Fact]
    public async Task Ack_after_multiple_retries_stops_delivery()
    {
        using var sub = await ConnectAsync("sub-late-ack");
        using var pub = await ConnectAsync("pub-late-ack");

        await SubscribeAsync(sub, "late/ack");
        await Task.Delay(100);

        // Publish QoS 1 — don't ACK
        await SendFrameAsync(pub, new Frame(
            Frame.Types.Publish, "late/ack", Qos: 1,
            Payload: JsonDocument.Parse("42").RootElement));

        // Receive initial delivery
        var first = await ReceiveFrameAsync(sub);
        Assert.NotNull(first);
        Assert.Equal(Frame.Types.Deliver, first!.Type);

        // Let several retries happen (100ms base in test config)
        var retryCount = 0;
        for (var i = 0; i < 3; i++)
        {
            var retry = await ReceiveFrameAsync(sub, TimeSpan.FromSeconds(2));
            Assert.NotNull(retry);
            Assert.Equal(first.MsgId, retry!.MsgId);
            Assert.Equal(42, retry.Payload.GetInt32());
            retryCount++;
        }
        Assert.Equal(3, retryCount);

        // NOW send ACK
        await SendFrameAsync(sub, new Frame(Frame.Types.Ack, MsgId: first.MsgId));
        await Task.Delay(50);

        // Outbox should be empty
        Assert.Empty(_broker.Qos.GetPending("sub-late-ack"));

        // No more deliveries should arrive
        try
        {
            var extra = await ReceiveFrameAsync(sub, TimeSpan.FromMilliseconds(500));
            Assert.True(extra is null || extra.MsgId != first.MsgId,
                "Should not receive more retries after ACK");
        }
        catch (OperationCanceledException)
        {
            // Expected — no more frames
        }
    }

    [Fact]
    public async Task Skip_2_then_ack_stops_retries()
    {
        using var sub = await ConnectAsync("sub-skip2");
        using var pub = await ConnectAsync("pub-skip2");

        await SubscribeAsync(sub, "skip/test");
        await Task.Delay(100);

        await SendFrameAsync(pub, new Frame(
            Frame.Types.Publish, "skip/test", Qos: 1,
            Payload: JsonDocument.Parse("77").RootElement));

        // Receive initial delivery — skip
        var first = await ReceiveFrameAsync(sub);
        Assert.NotNull(first);
        Assert.Equal(77, first!.Payload.GetInt32());

        // Skip retry #1
        var r1 = await ReceiveFrameAsync(sub, TimeSpan.FromSeconds(2));
        Assert.Equal(first.MsgId, r1!.MsgId);

        // Skip retry #2
        var r2 = await ReceiveFrameAsync(sub, TimeSpan.FromSeconds(2));
        Assert.Equal(first.MsgId, r2!.MsgId);

        // ACK on 3rd delivery (after 2 skips)
        await SendFrameAsync(sub, new Frame(Frame.Types.Ack, MsgId: first.MsgId));
        await Task.Delay(50);

        Assert.Empty(_broker.Qos.GetPending("sub-skip2"));

        // Confirm no more retries arrive
        try
        {
            var extra = await ReceiveFrameAsync(sub, TimeSpan.FromMilliseconds(500));
            Assert.True(extra is null || extra.MsgId != first.MsgId,
                "Should not receive retries after ACK");
        }
        catch (OperationCanceledException) { }
    }

    [Fact]
    public async Task Skip_then_new_value_resets_outbox()
    {
        using var sub = await ConnectAsync("sub-skip-reset");
        using var pub = await ConnectAsync("pub-skip-reset");

        await SubscribeAsync(sub, "reset/val");
        await Task.Delay(100);

        // Publish v1, skip its retries
        await SendFrameAsync(pub, new Frame(
            Frame.Types.Publish, "reset/val", Qos: 1,
            Payload: JsonDocument.Parse("100").RootElement));
        var d1 = await ReceiveFrameAsync(sub);
        Assert.Equal(100, d1!.Payload.GetInt32());

        // Let one retry arrive — still v1
        var r1 = await ReceiveFrameAsync(sub, TimeSpan.FromSeconds(2));
        Assert.Equal(d1.MsgId, r1!.MsgId);
        Assert.Equal(100, r1.Payload.GetInt32());

        // Publish v2 — replaces v1 in outbox
        await SendFrameAsync(pub, new Frame(
            Frame.Types.Publish, "reset/val", Qos: 1,
            Payload: JsonDocument.Parse("200").RootElement));
        var d2 = await ReceiveFrameAsync(sub);
        Assert.Equal(200, d2!.Payload.GetInt32());
        Assert.NotEqual(d1.MsgId, d2.MsgId);

        // Outbox should have only v2
        await Task.Delay(50);
        var pending = _broker.Qos.GetPending("sub-skip-reset");
        Assert.Single(pending);
        Assert.Equal(200, pending[0].Payload.GetInt32());

        // Retry should be v2 only
        var r2 = await ReceiveFrameAsync(sub, TimeSpan.FromSeconds(2));
        Assert.Equal(d2.MsgId, r2!.MsgId);
        Assert.Equal(200, r2.Payload.GetInt32());

        // ACK v2 — stops everything
        await SendFrameAsync(sub, new Frame(Frame.Types.Ack, MsgId: d2.MsgId));
        await Task.Delay(50);
        Assert.Empty(_broker.Qos.GetPending("sub-skip-reset"));
    }

    [Fact]
    public async Task Ack_stale_msgId_during_retries_keeps_current_pending()
    {
        using var sub = await ConnectAsync("sub-stale-retry");
        using var pub = await ConnectAsync("pub-stale-retry");

        await SubscribeAsync(sub, "stale/retry");
        await Task.Delay(100);

        // Publish v1
        await SendFrameAsync(pub, new Frame(
            Frame.Types.Publish, "stale/retry", Qos: 1,
            Payload: JsonDocument.Parse("1").RootElement));
        var d1 = await ReceiveFrameAsync(sub);

        // Publish v2 — replaces v1
        await SendFrameAsync(pub, new Frame(
            Frame.Types.Publish, "stale/retry", Qos: 1,
            Payload: JsonDocument.Parse("2").RootElement));
        var d2 = await ReceiveFrameAsync(sub);

        // Wait for a retry of v2
        var retry = await ReceiveFrameAsync(sub, TimeSpan.FromSeconds(2));
        Assert.Equal(d2!.MsgId, retry!.MsgId);

        // ACK stale v1 — no-op, v2 still pending
        await SendFrameAsync(sub, new Frame(Frame.Types.Ack, MsgId: d1!.MsgId));
        await Task.Delay(100);

        var pending = _broker.Qos.GetPending("sub-stale-retry");
        Assert.Single(pending);
        Assert.Equal(d2.MsgId, pending[0].MsgId);

        // ACK v2 — now empty
        await SendFrameAsync(sub, new Frame(Frame.Types.Ack, MsgId: d2.MsgId));
        await Task.Delay(50);
        Assert.Empty(_broker.Qos.GetPending("sub-stale-retry"));
    }

    [Fact]
    public async Task No_duplicate_on_reconnect_with_pending_outbox()
    {
        using var pub = await ConnectAsync("pub-nodup");

        // First connection — subscribe FIRST, then publish QoS 1 retained
        // so the publish fans out through the router into the outbox
        var ws1 = await ConnectAsync("sub-nodup");
        await SubscribeAsync(ws1, "nodup/#");
        await Task.Delay(100);

        await SendFrameAsync(pub, new Frame(
            Frame.Types.Publish, "nodup/topic", Qos: 1, Retain: true,
            Payload: JsonDocument.Parse("55").RootElement));

        // Receive the QoS 1 deliver (has MsgId, in outbox) — do NOT ACK
        var d1 = await ReceiveFrameAsync(ws1);
        Assert.NotNull(d1);
        Assert.Equal("nodup/topic", d1!.Topic);
        Assert.NotNull(d1.MsgId);

        // Disconnect
        await ws1.CloseAsync(
            System.Net.WebSockets.WebSocketCloseStatus.NormalClosure, null, CancellationToken.None);
        await Task.Delay(300);

        // Verify outbox still has the pending frame
        var pendingBefore = _broker.Qos.GetPending("sub-nodup");
        Assert.Single(pendingBefore);

        // Second connection — same clientId, resubscribe
        using var ws2 = await ConnectAsync("sub-nodup");
        await SubscribeAsync(ws2, "nodup/#");

        // Should receive exactly ONE delivery — the fresh retained push (replaced stale entry)
        var d2 = await ReceiveFrameAsync(ws2);
        Assert.NotNull(d2);
        Assert.Equal("nodup/topic", d2!.Topic);
        Assert.True(d2.Retained);
        Assert.Equal(55, d2.Payload.GetInt32());
        Assert.NotNull(d2.MsgId);
        Assert.NotEqual(d1.MsgId, d2.MsgId); // new MsgId from fresh EnqueueAsync

        // Outbox has the retained push (QoS 1, needs ACK)
        await Task.Delay(50);
        var pendingAfter = _broker.Qos.GetPending("sub-nodup");
        Assert.Single(pendingAfter);
        Assert.Equal(d2.MsgId, pendingAfter[0].MsgId);

        // ACK clears it
        await SendFrameAsync(ws2, new Frame(Frame.Types.Ack, MsgId: d2.MsgId));
        await Task.Delay(50);
        Assert.Empty(_broker.Qos.GetPending("sub-nodup"));

        // No duplicate frame should arrive
        try
        {
            var extra = await ReceiveFrameAsync(ws2, TimeSpan.FromMilliseconds(500));
            Assert.True(extra is null || extra.Topic != "nodup/topic",
                "Should not receive duplicate delivery after reconnect");
        }
        catch (OperationCanceledException) { }
    }

    [Fact]
    public async Task Retained_push_retried_until_acked()
    {
        using var pub = await ConnectAsync("pub-ret-qos");

        // Publish retained state
        await SendFrameAsync(pub, new Frame(
            Frame.Types.Publish, "retqos/val", Retain: true,
            Payload: JsonDocument.Parse("88").RootElement));
        await Task.Delay(100);

        // Subscribe — retained push is QoS 1
        using var sub = await ConnectAsync("sub-ret-qos");
        await SubscribeAsync(sub, "retqos/#");

        var first = await ReceiveFrameAsync(sub);
        Assert.NotNull(first);
        Assert.True(first!.Retained);
        Assert.Equal(1, first.Qos);
        Assert.NotNull(first.MsgId);

        // Don't ACK — wait for retry
        var retry = await ReceiveFrameAsync(sub, TimeSpan.FromSeconds(2));
        Assert.NotNull(retry);
        Assert.Equal(first.MsgId, retry!.MsgId);
        Assert.Equal(88, retry.Payload.GetInt32());

        // ACK stops retries
        await SendFrameAsync(sub, new Frame(Frame.Types.Ack, MsgId: first.MsgId));
        await Task.Delay(50);
        Assert.Empty(_broker.Qos.GetPending("sub-ret-qos"));
    }

    // ── PUBACK ──

    [Fact]
    public async Task Qos1_publish_receives_puback()
    {
        using var pub = await ConnectAsync("pub-puback");

        // Publish QoS 1 with MsgId
        await SendFrameAsync(pub, new Frame(
            Frame.Types.Publish, "puback/test", Qos: 1,
            MsgId: "pub-msg-1",
            Payload: JsonDocument.Parse("1").RootElement));

        // Should receive ACK back with same MsgId
        var ack = await ReceiveFrameAsync(pub);
        Assert.NotNull(ack);
        Assert.Equal(Frame.Types.Ack, ack!.Type);
        Assert.Equal("pub-msg-1", ack.MsgId);
    }

    [Fact]
    public async Task Qos0_publish_no_puback()
    {
        using var pub = await ConnectAsync("pub-no-puback");

        // Publish QoS 0 — no ACK expected
        await SendFrameAsync(pub, new Frame(
            Frame.Types.Publish, "noack/test", Qos: 0,
            Payload: JsonDocument.Parse("1").RootElement));

        // Send a PING to verify no ACK is queued ahead of it
        await SendFrameAsync(pub, new Frame(Frame.Types.Ping));

        var response = await ReceiveFrameAsync(pub);
        Assert.Equal(Frame.Types.Pong, response!.Type);
    }

    // ── SUBACK ──

    [Fact]
    public async Task Subscribe_receives_suback()
    {
        using var sub = await ConnectAsync("sub-suback");

        await SendFrameAsync(sub, new Frame(Frame.Types.Subscribe, "suback/test"));

        var response = await ReceiveFrameAsync(sub);
        Assert.NotNull(response);
        Assert.Equal(Frame.Types.SubAck, response!.Type);
        Assert.Equal("suback/test", response.Topic);
    }

    [Fact]
    public async Task Subscribe_suback_before_retained_push()
    {
        using var pub = await ConnectAsync("pub-suback-order");

        // Publish retained state first
        await SendFrameAsync(pub, new Frame(
            Frame.Types.Publish, "order/topic", Retain: true,
            Payload: JsonDocument.Parse("99").RootElement));
        await Task.Delay(100);

        using var sub = await ConnectAsync("sub-suback-order");
        await SendFrameAsync(sub, new Frame(Frame.Types.Subscribe, "order/#"));

        // SUBACK should come first
        var first = await ReceiveFrameAsync(sub);
        Assert.Equal(Frame.Types.SubAck, first!.Type);

        // Then retained state push
        var second = await ReceiveFrameAsync(sub);
        Assert.Equal(Frame.Types.Deliver, second!.Type);
        Assert.True(second.Retained);
    }

    // ── Retained delete ──

    [Fact]
    public async Task Publish_empty_payload_deletes_retained()
    {
        using var pub = await ConnectAsync("pub-del");

        // Set retained state
        await SendFrameAsync(pub, new Frame(
            Frame.Types.Publish, "del/topic", Retain: true,
            Payload: JsonDocument.Parse("123").RootElement));
        await Task.Delay(100);
        Assert.NotNull(_broker.Store.Get("del/topic"));

        // Delete by publishing empty payload with retain
        await SendFrameAsync(pub, new Frame(
            Frame.Types.Publish, "del/topic", Retain: true));
        await Task.Delay(100);

        Assert.Null(_broker.Store.Get("del/topic"));
    }

    [Fact]
    public async Task Deleted_retained_not_pushed_on_subscribe()
    {
        using var pub = await ConnectAsync("pub-del-sub");

        // Set then delete
        await SendFrameAsync(pub, new Frame(
            Frame.Types.Publish, "delsub/topic", Retain: true,
            Payload: JsonDocument.Parse("1").RootElement));
        await Task.Delay(50);
        await SendFrameAsync(pub, new Frame(
            Frame.Types.Publish, "delsub/topic", Retain: true));
        await Task.Delay(100);

        // Subscribe — SUBACK but no retained DELIVER (state was deleted)
        using var sub = await ConnectAsync("sub-del-sub");
        await SubscribeAsync(sub, "delsub/#");

        // No DELIVER should follow
        try
        {
            var extra = await ReceiveFrameAsync(sub, TimeSpan.FromMilliseconds(500));
            Assert.True(extra is null, "Should not receive retained state after delete");
        }
        catch (OperationCanceledException) { }
    }

    // ── DUP flag ──

    [Fact]
    public async Task Retry_sets_dup_flag()
    {
        using var sub = await ConnectAsync("sub-dup");
        using var pub = await ConnectAsync("pub-dup");

        await SubscribeAsync(sub, "dup/test");
        await Task.Delay(100);

        await SendFrameAsync(pub, new Frame(
            Frame.Types.Publish, "dup/test", Qos: 1,
            Payload: JsonDocument.Parse("1").RootElement));

        // First delivery — Dup=false
        var first = await ReceiveFrameAsync(sub);
        Assert.NotNull(first);
        Assert.False(first!.Dup);

        // Don't ACK — wait for retry
        var retry = await ReceiveFrameAsync(sub, TimeSpan.FromSeconds(2));
        Assert.NotNull(retry);
        Assert.True(retry!.Dup);
        Assert.Equal(first.MsgId, retry.MsgId);

        // ACK to clean up
        await SendFrameAsync(sub, new Frame(Frame.Types.Ack, MsgId: first.MsgId));
    }
}
