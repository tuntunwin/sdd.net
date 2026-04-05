namespace StateBroker.Core;

public interface IConsensusLayer
{
    ValueTask ProposeAsync(ReadOnlyMemory<byte> data, CancellationToken ct);

    bool IsLeader { get; }

    /// <summary>
    /// True when state mutations must go through ProposeAsync (Raft mode).
    /// False when the broker writes to StateStore directly (single-node mode).
    /// </summary>
    bool RequiresProposal { get; }

    /// <summary>
    /// Current leader's endpoint address, or null if unknown/single-node.
    /// Used to populate REDIRECT frames for followers.
    /// </summary>
    string? LeaderEndpoint { get; }
}

public sealed class NoopConsensus : IConsensusLayer
{
    public ValueTask ProposeAsync(ReadOnlyMemory<byte> data, CancellationToken ct) =>
        ValueTask.CompletedTask;

    public bool IsLeader => true;

    public bool RequiresProposal => false;

    public string? LeaderEndpoint => null;
}
