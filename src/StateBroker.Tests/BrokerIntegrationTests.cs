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
        var qos = new QosEngine();

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
        await SendFrameAsync(sub, new Frame(Frame.Types.Subscribe, "events/+"));
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
        await SendFrameAsync(sub, new Frame(Frame.Types.Subscribe, "state/#"));

        var deliver = await ReceiveFrameAsync(sub);
        Assert.NotNull(deliver);
        Assert.Equal(Frame.Types.Deliver, deliver!.Type);
        Assert.Equal("state/counter", deliver.Topic);
        Assert.True(deliver.Retained);
        Assert.Equal(100, deliver.Payload.GetInt32());
    }

    [Fact]
    public async Task Publish_qos1_creates_outbox_entry()
    {
        using var sub = await ConnectAsync("sub-qos1");
        using var pub = await ConnectAsync("pub-qos1");

        await SendFrameAsync(sub, new Frame(Frame.Types.Subscribe, "qos/test"));
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

        await SendFrameAsync(sub, new Frame(Frame.Types.Subscribe, "ack/test"));
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

        await SendFrameAsync(sub1, new Frame(Frame.Types.Subscribe, "fan/out"));
        await SendFrameAsync(sub2, new Frame(Frame.Types.Subscribe, "fan/out"));
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

        await SendFrameAsync(sub, new Frame(Frame.Types.Subscribe, "sensors/#"));
        await Task.Delay(100);

        var payload = JsonDocument.Parse("25").RootElement;
        await SendFrameAsync(pub, new Frame(
            Frame.Types.Publish, "sensors/temp/living", Payload: payload));

        var deliver = await ReceiveFrameAsync(sub);
        Assert.Equal(Frame.Types.Deliver, deliver!.Type);
        Assert.Equal("sensors/temp/living", deliver.Topic);
    }
}
