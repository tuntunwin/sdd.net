namespace StateBroker.Core;

public interface IQosEngine
{
    ValueTask EnqueueAsync(string clientId, Frame frame, CancellationToken ct);

    ValueTask AckAsync(string clientId, string msgId, CancellationToken ct);

    IReadOnlyList<Frame> GetPending(string clientId);

    ValueTask EvictAsync(string clientId, CancellationToken ct);

    /// <summary>
    /// Start a background retry loop for a client. Re-sends un-ACKed frames
    /// with exponential backoff. Cancels when ct is triggered (client disconnect).
    /// </summary>
    Task StartRetryLoopAsync(string clientId, SessionManager sessions, CancellationToken ct);
}
