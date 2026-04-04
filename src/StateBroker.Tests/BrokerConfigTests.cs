using System.Text.Json;
using StateBroker.Core;

namespace StateBroker.Tests;

public class BrokerConfigTests
{
    [Fact]
    public void Default_config_values()
    {
        var cfg = new BrokerConfig();

        Assert.Equal(":8080", cfg.Addr);
    }

    [Fact]
    public void StoreConfig_defaults()
    {
        var store = new StoreConfig();

        Assert.Equal("wal", store.Mode);
        Assert.Equal("./data", store.WalDir);
        Assert.Equal(64, store.SegmentMb);
        Assert.Equal(2, store.FsyncIntervalMs);
        Assert.Equal(64, store.Shards);
    }

    [Fact]
    public void QosConfig_defaults()
    {
        var qos = new QosConfig();

        Assert.Equal(TimeSpan.FromSeconds(1), qos.RetryBase);
        Assert.Equal(TimeSpan.FromMinutes(1), qos.RetryMax);
        Assert.Equal(TimeSpan.FromHours(24), qos.SessionTtl);
    }

    [Fact]
    public void SessionConfig_defaults()
    {
        var session = new SessionConfig();

        Assert.Equal(256, session.SendBufferDepth);
        Assert.Equal(TimeSpan.FromSeconds(30), session.EffectivePingInterval);
    }

    [Fact]
    public void SessionConfig_explicit_ping_interval()
    {
        var session = new SessionConfig(PingInterval: TimeSpan.FromSeconds(10));

        Assert.Equal(TimeSpan.FromSeconds(10), session.EffectivePingInterval);
    }

    [Fact]
    public void ClusterConfig_defaults()
    {
        var cluster = new ClusterConfig();

        Assert.Equal("single", cluster.Mode);
        Assert.Null(cluster.LocalEndpoint);
    }

    [Fact]
    public void Load_from_json_file()
    {
        var json = """
        {
            "addr": ":9090",
            "store": {
                "mode": "noop",
                "walDir": "/tmp/wal",
                "segmentMb": 128,
                "fsyncIntervalMs": 5,
                "shards": 32
            },
            "qos": {
                "retryBase": "00:00:02",
                "retryMax": "00:02:00",
                "sessionTtl": "12:00:00"
            },
            "session": {
                "sendBufferDepth": 512,
                "pingInterval": "00:00:15"
            },
            "cluster": {
                "mode": "raft",
                "localEndpoint": "10.0.0.1:9000",
                "peers": ["10.0.0.2:9000", "10.0.0.3:9000"],
                "raftElectionTimeout": "00:00:00.200"
            }
        }
        """;

        var path = Path.GetTempFileName();
        try
        {
            File.WriteAllText(path, json);
            var cfg = BrokerConfig.Load(path);

            Assert.Equal(":9090", cfg.Addr);

            Assert.Equal("noop", cfg.Store.Mode);
            Assert.Equal("/tmp/wal", cfg.Store.WalDir);
            Assert.Equal(128, cfg.Store.SegmentMb);
            Assert.Equal(5, cfg.Store.FsyncIntervalMs);
            Assert.Equal(32, cfg.Store.Shards);

            Assert.Equal(TimeSpan.FromSeconds(2), cfg.Qos.RetryBase);
            Assert.Equal(TimeSpan.FromMinutes(2), cfg.Qos.RetryMax);
            Assert.Equal(TimeSpan.FromHours(12), cfg.Qos.SessionTtl);

            Assert.Equal(512, cfg.Session.SendBufferDepth);
            Assert.Equal(TimeSpan.FromSeconds(15), cfg.Session.PingInterval);

            Assert.Equal("raft", cfg.Cluster.Mode);
            Assert.Equal("10.0.0.1:9000", cfg.Cluster.LocalEndpoint);
            Assert.Equal(2, cfg.Cluster.Peers.Length);
            Assert.Equal(TimeSpan.FromMilliseconds(200), cfg.Cluster.RaftElectionTimeout);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public void Load_minimal_json()
    {
        var path = Path.GetTempFileName();
        try
        {
            File.WriteAllText(path, "{}");
            var cfg = BrokerConfig.Load(path);

            Assert.NotNull(cfg);
            Assert.Equal(":8080", cfg.Addr);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public void Config_json_round_trip()
    {
        var cfg = new BrokerConfig(
            Addr: ":7070",
            Store: new StoreConfig(Mode: "wal", Shards: 16),
            Qos: new QosConfig(),
            Session: new SessionConfig(),
            Cluster: new ClusterConfig());

        var bytes = JsonSerializer.SerializeToUtf8Bytes(cfg, BrokerJsonContext.Default.BrokerConfig);
        var back = JsonSerializer.Deserialize(bytes, BrokerJsonContext.Default.BrokerConfig)!;

        Assert.Equal(":7070", back.Addr);
        Assert.Equal("wal", back.Store.Mode);
        Assert.Equal(16, back.Store.Shards);
    }
}
