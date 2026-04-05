#if ENABLE_RAFT
using DotNext.Net.Cluster.Consensus.Raft;
using StateBroker.Core;

namespace StateBroker.Raft;

public sealed class RaftConsensusLayer : IConsensusLayer
{
    private readonly RaftCluster _cluster;

    public RaftConsensusLayer(RaftCluster cluster)
    {
        _cluster = cluster;
    }

    public bool IsLeader =>
        !_cluster.LeadershipToken.IsCancellationRequested
        && _cluster.Leader is { } leader
        && leader.EndPoint.Equals(_cluster.LocalMemberAddress);

    public bool RequiresProposal => true;

    public string? LeaderEndpoint =>
        _cluster.Leader?.EndPoint.ToString();

    public async ValueTask ProposeAsync(ReadOnlyMemory<byte> data, CancellationToken ct)
    {
        if (!IsLeader)
            throw new NotLeaderException(LeaderEndpoint);

        var entry = new BrokerLogEntry(data) { Term = _cluster.Term };
        await _cluster.ReplicateAsync(entry, ct);
    }
}
#endif
