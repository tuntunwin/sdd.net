using System.Net.WebSockets;
using System.Text.Json;
using StateBroker.Core;

var broker = "ws://localhost:8080/";
var mode = args.Length > 0 ? args[0] : "both";
var topic = args.Length > 1 ? args[1] : "test/greeting";

switch (mode)
{
    case "sub":
        await RunSubscriber(broker, topic, skip: -1); // always ACK
        break;
    case "sub-noack":
        var skip = args.Length > 2 && int.TryParse(args[2], out var s) ? s : 0;
        await RunSubscriber(broker, topic, skip: skip);
        break;
    case "pub":
        await RunPublisher(broker, topic);
        break;
    case "both":
        await RunBoth(broker, topic);
        break;
    default:
        Console.WriteLine("Usage: dotnet run -- <mode> [topic] [skip]");
        Console.WriteLine();
        Console.WriteLine("Modes:");
        Console.WriteLine("  sub            — subscribe and ACK every message");
        Console.WriteLine("  sub-noack      — skip ACK, then ACK after [skip] retries (0 = never ACK)");
        Console.WriteLine("  pub            — publish messages from stdin");
        Console.WriteLine("  both           — demo: pub/sub with retained state");
        Console.WriteLine();
        Console.WriteLine("Examples:");
        Console.WriteLine("  dotnet run -- sub-noack \"sensors/#\"      # never ACK");
        Console.WriteLine("  dotnet run -- sub-noack \"sensors/#\" 3    # ACK after skipping 3");
        break;
}

// ── Subscriber ──────────────────────────────────────────────────

/// <param name="skip">
/// -1 = always ACK immediately (sub mode).
///  0 = never ACK (sub-noack default).
/// >0 = skip N deliveries per msgId, then ACK.
/// </param>
static async Task RunSubscriber(string broker, string topic, int skip)
{
    var alwaysAck = skip < 0;
    var label = alwaysAck ? "sub" : "sub-noack";
    var clientId = alwaysAck ? "subscriber-1" : "subscriber-noack";
    using var ws = await ConnectAsync(broker, clientId);
    Console.WriteLine($"[{label}] Connected as {clientId}");

    await SendAsync(ws, new Frame(Frame.Types.Subscribe, topic));
    Console.WriteLine($"[{label}] Subscribed to: {topic}");
    if (!alwaysAck)
    {
        if (skip == 0)
            Console.WriteLine($"[{label}] *** ACK disabled — never ACK ***");
        else
            Console.WriteLine($"[{label}] *** ACK after skipping {skip} per message ***");
    }
    Console.WriteLine($"[{label}] Waiting for messages (Ctrl+C to quit)...\n");

    using var cts = new CancellationTokenSource();
    Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };

    var received = 0;
    // Track skip count per msgId
    var skipCounts = new Dictionary<string, int>();

    try
    {
        while (!cts.Token.IsCancellationRequested)
        {
            var frame = await ReceiveAsync(ws, cts.Token);
            if (frame is null) break;

            switch (frame.Type)
            {
                case Frame.Types.Deliver:
                    received++;
                    var retainedTag = frame.Retained ? " [retained]" : "";
                    var qosTag = frame.Qos > 0 ? $" QoS={frame.Qos}" : "";
                    Console.WriteLine($"[{label}] #{received} DELIVER {frame.Topic}{retainedTag}{qosTag}: {frame.Payload}");

                    if (frame.MsgId is not null)
                    {
                        if (alwaysAck)
                        {
                            await SendAsync(ws, new Frame(Frame.Types.Ack, MsgId: frame.MsgId));
                            Console.WriteLine($"[{label}]   ACK {frame.MsgId}");
                        }
                        else if (skip > 0)
                        {
                            skipCounts.TryGetValue(frame.MsgId, out var seen);
                            seen++;
                            if (seen > skip)
                            {
                                await SendAsync(ws, new Frame(Frame.Types.Ack, MsgId: frame.MsgId));
                                skipCounts.Remove(frame.MsgId);
                                Console.WriteLine($"[{label}]   ACK {frame.MsgId} (after {skip} skips)");
                            }
                            else
                            {
                                skipCounts[frame.MsgId] = seen;
                                Console.WriteLine($"[{label}]   SKIP {seen}/{skip} {frame.MsgId}");
                            }
                        }
                        else
                        {
                            Console.WriteLine($"[{label}]   NO-ACK {frame.MsgId}");
                        }
                    }
                    break;

                case Frame.Types.Pong:
                    Console.WriteLine($"[{label}] PONG");
                    break;

                default:
                    Console.WriteLine($"[{label}] {frame.Type}: {frame.Payload}");
                    break;
            }
        }
    }
    catch (OperationCanceledException) { }

    Console.WriteLine($"[{label}] Disconnected. received={received} pending_skips={skipCounts.Count}");
}

// ── Publisher ────────────────────────────────────────────────────

static async Task RunPublisher(string broker, string topic)
{
    using var ws = await ConnectAsync(broker, "publisher-1");
    Console.WriteLine($"[pub] Connected as publisher-1");
    Console.WriteLine($"[pub] Publishing to: {topic}");
    Console.WriteLine("[pub] Type a message and press Enter (empty line to quit):\n");

    while (true)
    {
        Console.Write("[pub] > ");
        var line = Console.ReadLine();
        if (string.IsNullOrEmpty(line)) break;

        JsonElement payload;
        try
        {
            payload = JsonDocument.Parse(line).RootElement;
        }
        catch (JsonException)
        {
            // Wrap plain text as a JSON string
            payload = JsonDocument.Parse($"\"{line}\"").RootElement;
        }

        await SendAsync(ws, new Frame(
            Frame.Types.Publish, topic, Qos: 1, Retain: true, Payload: payload));
        Console.WriteLine($"[pub] Published to {topic}: {payload}");
    }

    Console.WriteLine("[pub] Done.");
}

// ── Both (demo mode) ────────────────────────────────────────────

static async Task RunBoth(string broker, string topic)
{
    Console.WriteLine("=== StateBroker Test Client — Demo Mode ===\n");

    // Connect subscriber
    using var sub = await ConnectAsync(broker, "demo-sub");
    Console.WriteLine("[sub] Connected as demo-sub");

    // Connect publisher
    using var pub = await ConnectAsync(broker, "demo-pub");
    Console.WriteLine("[pub] Connected as demo-pub");

    // Ping
    await SendAsync(pub, new Frame(Frame.Types.Ping));
    var pong = await ReceiveAsync(pub, CancellationToken.None, TimeSpan.FromSeconds(5));
    Console.WriteLine($"[pub] PING → {pong?.Type}\n");

    // Subscribe
    await SendAsync(sub, new Frame(Frame.Types.Subscribe, topic));
    Console.WriteLine($"[sub] Subscribed to: {topic}");
    await Task.Delay(100);

    // Publish a few messages
    var messages = new[]
    {
        """{"sensor":"temp","value":22.5,"unit":"°C"}""",
        """{"sensor":"humidity","value":45,"unit":"%"}""",
        """{"sensor":"temp","value":23.1,"unit":"°C"}""",
    };

    foreach (var msg in messages)
    {
        var payload = JsonDocument.Parse(msg).RootElement;
        await SendAsync(pub, new Frame(
            Frame.Types.Publish, topic, Qos: 1, Retain: true, Payload: payload));
        Console.WriteLine($"[pub] Published: {msg}");

        // Receive delivery on subscriber
        var deliver = await ReceiveAsync(sub, CancellationToken.None, TimeSpan.FromSeconds(5));
        if (deliver is not null)
        {
            Console.WriteLine($"[sub] Received: {deliver.Payload}");
            if (deliver.MsgId is not null)
            {
                await SendAsync(sub, new Frame(Frame.Types.Ack, MsgId: deliver.MsgId));
                Console.WriteLine($"[sub] ACK: {deliver.MsgId}");
            }
        }
        Console.WriteLine();
        await Task.Delay(100);
    }

    // Demonstrate retained state — new subscriber gets latest
    Console.WriteLine("--- New subscriber joining (should receive retained state) ---\n");
    using var sub2 = await ConnectAsync(broker, "demo-sub2");
    await SendAsync(sub2, new Frame(Frame.Types.Subscribe, topic));

    var retained = await ReceiveAsync(sub2, CancellationToken.None, TimeSpan.FromSeconds(5));
    if (retained is not null)
    {
        Console.WriteLine($"[sub2] Retained: {retained.Payload} (retained={retained.Retained})");
        if (retained.MsgId is not null)
            await SendAsync(sub2, new Frame(Frame.Types.Ack, MsgId: retained.MsgId));
    }

    Console.WriteLine("\n=== Demo complete ===");
}

// ── Helpers ──────────────────────────────────────────────────────

static async Task<ClientWebSocket> ConnectAsync(string broker, string clientId)
{
    var ws = new ClientWebSocket();
    var uri = new Uri($"{broker}?clientId={clientId}");
    await ws.ConnectAsync(uri, CancellationToken.None);
    return ws;
}

static async Task SendAsync(ClientWebSocket ws, Frame frame)
{
    var bytes = JsonSerializer.SerializeToUtf8Bytes(frame, BrokerJsonContext.Default.Frame);
    await ws.SendAsync(bytes, WebSocketMessageType.Text, true, CancellationToken.None);
}

static async Task<Frame?> ReceiveAsync(
    ClientWebSocket ws, CancellationToken ct, TimeSpan? timeout = null)
{
    using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
    if (timeout is not null)
        cts.CancelAfter(timeout.Value);

    var buffer = new byte[8192];
    using var ms = new MemoryStream();

    WebSocketReceiveResult result;
    do
    {
        result = await ws.ReceiveAsync(buffer, cts.Token);
        ms.Write(buffer, 0, result.Count);
    } while (!result.EndOfMessage);

    if (result.MessageType == WebSocketMessageType.Close)
        return null;

    return JsonSerializer.Deserialize(ms.ToArray(), BrokerJsonContext.Default.Frame);
}
