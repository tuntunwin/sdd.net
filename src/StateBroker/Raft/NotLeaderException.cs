#if ENABLE_RAFT
namespace StateBroker.Raft;

public sealed class NotLeaderException(string? leaderEndpoint)
    : Exception($"This node is not the leader. Leader: {leaderEndpoint ?? "unknown"}")
{
    public string? LeaderEndpoint { get; } = leaderEndpoint;
}
#endif
