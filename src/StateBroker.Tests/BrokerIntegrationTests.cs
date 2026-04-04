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
        // Pick a random port in ephemeral range
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

        _broker = new Broker(transport, store, consensus, sessions);
        _brokerTask = _broker.RunAsync(_cts.Token);

        // Give the listener a moment to start
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

        // Both tracked in session manager
        await Task.Delay(100); // let broker register sessions
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

        // Give broker time to process
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

        // Should not crash the broker — verify broker still accepts connections
        using var ws2 = await ConnectAsync("after-close");
        Assert.Equal(WebSocketState.Open, ws2.State);
    }
}
