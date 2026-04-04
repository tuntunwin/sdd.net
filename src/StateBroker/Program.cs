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

// Step 3: in-memory mode — NullWal + NoopConsensus
await using var wal = new NullWalWriter();
var consensus = new NoopConsensus();
using var store = new ShardedStateStore(
    config.Store?.Shards ?? 64, wal);

var sessions = new SessionManager(
    config.Session?.SendBufferDepth ?? 256);

await using var transport = new WebSocketTransport(prefix);

var broker = new Broker(transport, store, consensus, sessions);

using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };

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
