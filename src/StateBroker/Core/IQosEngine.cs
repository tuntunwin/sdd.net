namespace StateBroker.Core;

public interface IQosEngine
{
    ValueTask EnqueueAsync(string clientId, Frame frame, CancellationToken ct);

    ValueTask AckAsync(string clientId, string msgId, CancellationToken ct);

    IReadOnlyList<Frame> GetPending(string clientId);

    ValueTask EvictAsync(string clientId, CancellationToken ct);
}
