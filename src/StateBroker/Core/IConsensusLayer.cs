namespace StateBroker.Core;

public interface IConsensusLayer
{
    ValueTask ProposeAsync(ReadOnlyMemory<byte> data, CancellationToken ct);

    bool IsLeader { get; }
}

public sealed class NoopConsensus : IConsensusLayer
{
    public ValueTask ProposeAsync(ReadOnlyMemory<byte> data, CancellationToken ct) =>
        ValueTask.CompletedTask;

    public bool IsLeader => true;
}
