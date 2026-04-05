using System.Net.WebSockets;
using System.Text.Json;
using StateBroker.Core;
using StateBroker.Transport;

namespace StateBroker.Tests;

/// <summary>
/// Tests for consensus-layer routing: REDIRECT on follower, single-node passthrough,
/// RequiresProposal behavior.
/// </summary>
public class ConsensusRoutingTests
{
    // ── NoopConsensus properties ──

    [Fact]
    public void NoopConsensus_RequiresProposal_false()
    {
        var noop = new NoopConsensus();
        Assert.False(noop.RequiresProposal);
    }

    [Fact]
    public void NoopConsensus_LeaderEndpoint_null()
    {
        var noop = new NoopConsensus();
        Assert.Null(noop.LeaderEndpoint);
    }

    // ── MockConsensusLayer ──

    [Fact]
    public void MockConsensus_as_leader()
    {
        var mock = new MockConsensusLayer(isLeader: true, leaderEndpoint: "10.0.0.1:9000");
        Assert.True(mock.IsLeader);
        Assert.True(mock.RequiresProposal);
        Assert.Equal("10.0.0.1:9000", mock.LeaderEndpoint);
    }

    [Fact]
    public void MockConsensus_as_follower()
    {
        var mock = new MockConsensusLayer(isLeader: false, leaderEndpoint: "10.0.0.1:9000");
        Assert.False(mock.IsLeader);
        Assert.True(mock.RequiresProposal);
    }

    [Fact]
    public async Task MockConsensus_leader_ProposeAsync_records_data()
    {
        var mock = new MockConsensusLayer(isLeader: true);
        var data = new byte[] { 1, 2, 3 };
        await mock.ProposeAsync(data, CancellationToken.None);

        Assert.Single(mock.Proposals);
        Assert.Equal(data, mock.Proposals[0].ToArray());
    }
}

/// <summary>
/// Integration tests for REDIRECT behavior when broker is a follower.
/// </summary>
public class RedirectIntegrationTests : IAsyncLifetime
{
    private readonly int _port;
    private readonly CancellationTokenSource _cts = new();
    private readonly MockConsensusLayer _consensus;
    private Broker _broker = null!;
    private Task _brokerTask = null!;

    public RedirectIntegrationTests()
    {
        var listener = new System.Net.Sockets.TcpListener(
            System.Net.IPAddress.Loopback, 0);
        listener.Start();
        _port = ((System.Net.IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();

        // Start as follower pointing to leader at 10.0.0.1:9000
        _consensus = new MockConsensusLayer(isLeader: false, leaderEndpoint: "10.0.0.1:9000");
    }

    public Task InitializeAsync()
    {
        var transport = new WebSocketTransport($"http://localhost:{_port}/");
        var store = new ShardedStateStore();
        var sessions = new SessionManager();
        var router = new TopicRouter();
        var qos = new QosEngine(TimeSpan.FromMilliseconds(100), TimeSpan.FromMilliseconds(500));

        _broker = new Broker(transport, store, _consensus, sessions, router, qos);
        _brokerTask = _broker.RunAsync(_cts.Token);
        return Task.Delay(200);
    }

    public async Task DisposeAsync()
    {
        _cts.Cancel();
        try { await _brokerTask; } catch (OperationCanceledException) { }
        _cts.Dispose();
    }

    private async Task<ClientWebSocket> ConnectAsync(string clientId)
    {
        var ws = new ClientWebSocket();
        await ws.ConnectAsync(
            new Uri($"ws://localhost:{_port}/?clientId={clientId}"),
            CancellationToken.None);
        return ws;
    }

    private static async Task SendFrameAsync(ClientWebSocket ws, Frame frame)
    {
        var bytes = JsonSerializer.SerializeToUtf8Bytes(frame, BrokerJsonContext.Default.Frame);
        await ws.SendAsync(bytes, WebSocketMessageType.Text, true, CancellationToken.None);
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
        if (result.MessageType == WebSocketMessageType.Close) return null;
        return JsonSerializer.Deserialize(ms.ToArray(), BrokerJsonContext.Default.Frame);
    }

    [Fact]
    public async Task Follower_redirects_retained_qos1_publish()
    {
        using var pub = await ConnectAsync("pub-redirect");

        await SendFrameAsync(pub, new Frame(
            Frame.Types.Publish, "state/topic", Qos: 1, MsgId: "m1",
            Retain: true,
            Payload: JsonDocument.Parse("42").RootElement));

        var response = await ReceiveFrameAsync(pub);
        Assert.NotNull(response);
        Assert.Equal(Frame.Types.Redirect, response!.Type);

        // Payload should contain leader endpoint
        var leader = response.Payload.GetProperty("leader").GetString();
        Assert.Equal("10.0.0.1:9000", leader);
    }

    [Fact]
    public async Task Follower_redirects_retained_delete()
    {
        using var pub = await ConnectAsync("pub-redirect-del");

        // Empty payload + retain = retained delete
        await SendFrameAsync(pub, new Frame(
            Frame.Types.Publish, "del/topic", Retain: true));

        var response = await ReceiveFrameAsync(pub);
        Assert.Equal(Frame.Types.Redirect, response!.Type);
    }

    [Fact]
    public async Task Follower_does_not_redirect_qos0()
    {
        using var sub = await ConnectAsync("sub-qos0");
        using var pub = await ConnectAsync("pub-qos0");

        // Subscribe first (SUBACK)
        await SendFrameAsync(sub, new Frame(Frame.Types.Subscribe, "qos0/test"));
        await ReceiveFrameAsync(sub); // drain SUBACK
        await Task.Delay(100);

        // QoS 0 publish — should fan out locally, no redirect
        await SendFrameAsync(pub, new Frame(
            Frame.Types.Publish, "qos0/test",
            Payload: JsonDocument.Parse("1").RootElement));

        var deliver = await ReceiveFrameAsync(sub);
        Assert.Equal(Frame.Types.Deliver, deliver!.Type);
        Assert.Equal(1, deliver.Payload.GetInt32());
    }

    [Fact]
    public async Task Follower_does_not_redirect_non_retained_qos1()
    {
        using var sub = await ConnectAsync("sub-nonretain");
        using var pub = await ConnectAsync("pub-nonretain");

        await SendFrameAsync(sub, new Frame(Frame.Types.Subscribe, "nr/test"));
        await ReceiveFrameAsync(sub); // SUBACK
        await Task.Delay(100);

        // Non-retained QoS 1 — local fan-out, no redirect
        await SendFrameAsync(pub, new Frame(
            Frame.Types.Publish, "nr/test", Qos: 1, MsgId: "m1",
            Payload: JsonDocument.Parse("1").RootElement));

        // Publisher gets PUBACK
        var ack = await ReceiveFrameAsync(pub);
        Assert.Equal(Frame.Types.Ack, ack!.Type);

        // Subscriber gets DELIVER
        var deliver = await ReceiveFrameAsync(sub);
        Assert.Equal(Frame.Types.Deliver, deliver!.Type);
    }

    [Fact]
    public async Task Follower_no_puback_on_redirect()
    {
        using var pub = await ConnectAsync("pub-no-puback");

        await SendFrameAsync(pub, new Frame(
            Frame.Types.Publish, "state/x", Qos: 1, MsgId: "m1",
            Retain: true,
            Payload: JsonDocument.Parse("1").RootElement));

        // Should get REDIRECT, not ACK
        var response = await ReceiveFrameAsync(pub);
        Assert.Equal(Frame.Types.Redirect, response!.Type);

        // Verify no ACK follows
        await SendFrameAsync(pub, new Frame(Frame.Types.Ping));
        var pong = await ReceiveFrameAsync(pub);
        Assert.Equal(Frame.Types.Pong, pong!.Type);
    }

    [Fact]
    public async Task Follower_does_not_store_redirected_publish()
    {
        using var pub = await ConnectAsync("pub-no-store");

        await SendFrameAsync(pub, new Frame(
            Frame.Types.Publish, "nostore/topic", Qos: 1, MsgId: "m1",
            Retain: true,
            Payload: JsonDocument.Parse("99").RootElement));

        await ReceiveFrameAsync(pub); // REDIRECT

        // StateStore should be empty — follower didn't store it
        Assert.Null(_broker.Store.Get("nostore/topic"));
    }

    [Fact]
    public async Task Ping_works_on_follower()
    {
        using var ws = await ConnectAsync("ping-follower");
        await SendFrameAsync(ws, new Frame(Frame.Types.Ping));
        var pong = await ReceiveFrameAsync(ws);
        Assert.Equal(Frame.Types.Pong, pong!.Type);
    }
}

/// <summary>
/// Mock consensus layer for testing leader/follower behavior.
/// </summary>
internal sealed class MockConsensusLayer : IConsensusLayer
{
    public bool IsLeader { get; set; }
    public bool RequiresProposal => true;
    public string? LeaderEndpoint { get; set; }
    public List<ReadOnlyMemory<byte>> Proposals { get; } = [];

    public MockConsensusLayer(bool isLeader, string? leaderEndpoint = null)
    {
        IsLeader = isLeader;
        LeaderEndpoint = leaderEndpoint;
    }

    public ValueTask ProposeAsync(ReadOnlyMemory<byte> data, CancellationToken ct)
    {
        if (!IsLeader)
            throw new InvalidOperationException("Not leader");

        lock (Proposals)
            Proposals.Add(data);

        return ValueTask.CompletedTask;
    }
}
