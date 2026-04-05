using System.Text.Json;

namespace StateBroker.Core;

public sealed record BrokerConfig(
    string        Addr    = ":8080",
    StoreConfig   Store   = default!,
    QosConfig     Qos     = default!,
    SessionConfig Session = default!,
    ClusterConfig Cluster = default!
)
{
    public static BrokerConfig Load(string path)
    {
        using var stream = File.OpenRead(path);
        return JsonSerializer.Deserialize(stream, BrokerJsonContext.Default.BrokerConfig)
            ?? new BrokerConfig();
    }
}

public sealed record StoreConfig(
    string   Mode             = "wal",
    string   WalDir           = "./data",
    int      SegmentMb        = 64,
    TimeSpan SnapshotInterval = default,
    int      FsyncIntervalMs  = 2,
    int      Shards           = 64
)
{
    public StoreConfig() : this(Mode: "wal") { }
}

public sealed record QosConfig(
    TimeSpan RetryBase  = default,
    TimeSpan RetryMax   = default,
    TimeSpan SessionTtl = default
)
{
    public QosConfig() : this(
        RetryBase:  TimeSpan.FromSeconds(1),
        RetryMax:   TimeSpan.FromMinutes(1),
        SessionTtl: TimeSpan.FromHours(24)) { }
}

public sealed record SessionConfig(
    int      SendBufferDepth = 256,
    TimeSpan PingInterval    = default
)
{
    public SessionConfig() : this(SendBufferDepth: 256)
    {
        // PingInterval defaults to 30s via property below
    }

    public TimeSpan EffectivePingInterval =>
        PingInterval == default ? TimeSpan.FromSeconds(30) : PingInterval;
}

public sealed record ClusterConfig(
    string   Mode                = "single",
    string[] Peers               = default!,
    string?  LocalEndpoint       = null,
    TimeSpan RaftElectionTimeout = default,
    string?  RaftLogDir          = null,
    int      HeartbeatThreshold  = 2,
    int      SnapshotThreshold   = 10000,
    TimeSpan SnapshotInterval    = default
)
{
    public ClusterConfig() : this(Mode: "single") { }

    public TimeSpan EffectiveSnapshotInterval =>
        SnapshotInterval == default ? TimeSpan.FromMinutes(10) : SnapshotInterval;
}
