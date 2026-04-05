#if ENABLE_RAFT
using System.Net;
using DotNext.Net.Cluster.Consensus.Raft;
using StateBroker.Core;

namespace StateBroker.Raft;

public static class RaftBootstrap
{
    public static async Task<(RaftConsensusLayer Consensus, BrokerStateMachine StateMachine)>
        CreateAsync(
            ClusterConfig config,
            IStateStore store,
            ISubscriberNotifier notifier,
            CancellationToken ct = default)
    {
        var logDir = config.RaftLogDir ?? "./data/raft";
        var sm = new BrokerStateMachine(logDir, store, notifier);

        var localEndpoint = IPEndPoint.Parse(
            config.LocalEndpoint
            ?? throw new ArgumentException("LocalEndpoint is required for Raft mode"));

        var tcpConfig = new RaftCluster.TcpConfiguration(localEndpoint)
        {
            ColdStart = config.Peers is null or { Length: 0 },
        };

        if (config.RaftElectionTimeout != default)
        {
            var ms = (int)config.RaftElectionTimeout.TotalMilliseconds;
            tcpConfig.LowerElectionTimeout = ms;
            tcpConfig.UpperElectionTimeout = ms * 2;
        }

        if (config.HeartbeatThreshold > 0)
            tcpConfig.HeartbeatThreshold = config.HeartbeatThreshold;

        var cluster = new RaftCluster(tcpConfig);
        cluster.AuditTrail = sm;

        await cluster.StartAsync(ct);

        // Add peers to the cluster (must be done after start on the leader)
        if (config.Peers is not null)
        {
            foreach (var peer in config.Peers)
            {
                var endpoint = IPEndPoint.Parse(peer);
                await cluster.AddMemberAsync(endpoint, ct);
            }
        }

        var consensus = new RaftConsensusLayer(cluster);
        return (consensus, sm);
    }
}
#endif
