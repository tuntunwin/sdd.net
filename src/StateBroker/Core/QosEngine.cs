using System.Collections.Concurrent;

namespace StateBroker.Core;

public sealed class QosEngine : IQosEngine
{
    private readonly ConcurrentDictionary<string, Outbox> _outboxes = new();
    private readonly TimeSpan _retryBase;
    private readonly TimeSpan _retryMax;

    public QosEngine(TimeSpan? retryBase = null, TimeSpan? retryMax = null)
    {
        _retryBase = retryBase ?? TimeSpan.FromSeconds(1);
        _retryMax = retryMax ?? TimeSpan.FromMinutes(1);
    }

    public async ValueTask EnqueueAsync(string clientId, Frame frame, CancellationToken ct)
    {
        if (frame.Topic is null)
            throw new ArgumentException("Frame.Topic is required for QoS outbox", nameof(frame));

        var outbox = GetOrCreateOutbox(clientId);
        await outbox.Lock.WaitAsync(ct);
        try
        {
            outbox.Pending[frame.Topic] = frame;
        }
        finally
        {
            outbox.Lock.Release();
        }
    }

    public async ValueTask AckAsync(string clientId, string msgId, CancellationToken ct)
    {
        if (!_outboxes.TryGetValue(clientId, out var outbox))
            return;

        await outbox.Lock.WaitAsync(ct);
        try
        {
            string? topicToRemove = null;
            foreach (var kvp in outbox.Pending)
            {
                if (kvp.Value.MsgId == msgId)
                {
                    topicToRemove = kvp.Key;
                    break;
                }
            }
            if (topicToRemove is not null)
                outbox.Pending.Remove(topicToRemove);
        }
        finally
        {
            outbox.Lock.Release();
        }
    }

    public IReadOnlyList<Frame> GetPending(string clientId)
    {
        if (!_outboxes.TryGetValue(clientId, out var outbox))
            return [];

        outbox.Lock.Wait();
        try
        {
            return outbox.Pending.Values.ToList();
        }
        finally
        {
            outbox.Lock.Release();
        }
    }

    public async ValueTask EvictAsync(string clientId, CancellationToken ct)
    {
        if (!_outboxes.TryRemove(clientId, out var outbox))
            return;

        await outbox.Lock.WaitAsync(ct);
        try
        {
            outbox.Pending.Clear();
        }
        finally
        {
            outbox.Lock.Release();
        }
    }

    public async Task StartRetryLoopAsync(
        string clientId, SessionManager sessions, CancellationToken ct)
    {
        var delay = _retryBase;

        try
        {
            while (!ct.IsCancellationRequested)
            {
                await Task.Delay(delay, ct);

                var pending = GetPending(clientId);
                if (pending.Count == 0)
                {
                    delay = _retryBase;
                    continue;
                }

                foreach (var frame in pending)
                    sessions.TrySend(clientId, frame);

                delay = TimeSpan.FromTicks(Math.Min(delay.Ticks * 2, _retryMax.Ticks));
            }
        }
        catch (OperationCanceledException) { }
    }

    private Outbox GetOrCreateOutbox(string clientId) =>
        _outboxes.GetOrAdd(clientId, _ => new Outbox());

    private sealed class Outbox
    {
        public SemaphoreSlim Lock { get; } = new(1, 1);
        public Dictionary<string, Frame> Pending { get; } = new();
    }
}
