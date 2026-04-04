using StateBroker;
using StateBroker.Core;
using StateBroker.Transport;

var configPath = args.Length > 0 ? args[0] : "appsettings.json";

var config = File.Exists(configPath)
    ? BrokerConfig.Load(configPath)
    : new BrokerConfig();

// Parse listen address — config.Addr is ":port" format
var port = config.Addr.TrimStart(':');
var prefix = $"http://localhost:{port}/";

// Resolve storage mode
IWalWriter wal;
Task? flushTask = null;
if (config.Store?.Mode == "wal")
{
    var walWriter = new WalWriter(
        config.Store.WalDir,
        config.Store.SegmentMb,
        config.Store.FsyncIntervalMs);
    wal = walWriter;
    // Flush loop started after broker setup
    flushTask = Task.CompletedTask; // placeholder, started below
}
else
{
    wal = new NullWalWriter();
}

var consensus = new NoopConsensus();
using var store = new ShardedStateStore(
    config.Store?.Shards ?? 64, wal);

// Replay WAL into StateStore on startup
await foreach (var entry in wal.ReplayAsync(CancellationToken.None))
    store.SetMemory(entry.Topic, entry.Payload);

var sessions = new SessionManager(
    config.Session?.SendBufferDepth ?? 256);
using var router = new TopicRouter();
var qos = new QosEngine();

await using var transport = new WebSocketTransport(prefix);

var broker = new Broker(transport, store, consensus, sessions, router, qos);

using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };

// Start WAL flush loop if in WAL mode
if (wal is WalWriter walWriter2)
    flushTask = walWriter2.FlushLoopAsync(cts.Token);

Console.WriteLine($"StateBroker listening on {prefix}");
Console.WriteLine($"  store.mode={config.Store?.Mode ?? "noop"}  cluster.mode={config.Cluster?.Mode ?? "single"}");
Console.WriteLine("  Press Ctrl+C to stop.");

try
{
    await broker.RunAsync(cts.Token);
}
catch (OperationCanceledException)
{
    Console.WriteLine("Shutting down.");
}

await wal.DisposeAsync();
