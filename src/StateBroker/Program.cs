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

// Shared components
using var store = new ShardedStateStore(config.Store?.Shards ?? 64);
var sessions = new SessionManager(config.Session?.SendBufferDepth ?? 256);
using var router = new TopicRouter();
var qos = new QosEngine(config.Qos?.RetryBase, config.Qos?.RetryMax);

IWalWriter wal;
IConsensusLayer consensus;
Task? flushTask = null;

#if ENABLE_RAFT
if (config.Cluster?.Mode == "raft")
{
    // Raft mode: Raft log IS the WAL — inject NullWalWriter
    wal = new NullWalWriter();
    store.ConfigureWal(wal);

    var notifier = new StateBroker.Raft.SubscriberNotifier(router, sessions);
    var (raftConsensus, stateMachine) = await StateBroker.Raft.RaftBootstrap.CreateAsync(
        config.Cluster, store, notifier);
    consensus = raftConsensus;

    Console.WriteLine($"StateBroker [Raft] listening on {prefix}");
    Console.WriteLine($"  local={config.Cluster.LocalEndpoint}  peers={string.Join(",", config.Cluster.Peers ?? [])}");
}
else
#endif
{
    // Single-node mode
    if (config.Store?.Mode == "wal")
    {
        var walWriter = new WalWriter(
            config.Store.WalDir,
            config.Store.SegmentMb,
            config.Store.FsyncIntervalMs);
        wal = walWriter;
        flushTask = Task.CompletedTask; // placeholder, started below
    }
    else
    {
        wal = new NullWalWriter();
    }

    store.ConfigureWal(wal);
    consensus = new NoopConsensus();

    // Replay WAL into StateStore on startup
    await foreach (var entry in wal.ReplayAsync(CancellationToken.None))
    {
        if (entry.Delete)
            store.DeleteMemory(entry.Topic);
        else
            store.SetMemory(entry.Topic, entry.Payload);
    }

    Console.WriteLine($"StateBroker listening on {prefix}");
    Console.WriteLine($"  store.mode={config.Store?.Mode ?? "noop"}  cluster.mode=single");
}

await using var transport = new WebSocketTransport(prefix);
var broker = new Broker(transport, store, consensus, sessions, router, qos);

using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };

// Start WAL flush loop if in WAL mode
if (wal is WalWriter walWriter2)
    flushTask = walWriter2.FlushLoopAsync(cts.Token);

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
