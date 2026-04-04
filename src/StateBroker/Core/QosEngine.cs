using System.Collections.Concurrent;

namespace StateBroker.Core;

public sealed class QosEngine : IQosEngine
{
    private readonly ConcurrentDictionary<string, Outbox> _outboxes = new();

    public async ValueTask EnqueueAsync(string clientId, Frame frame, CancellationToken ct)
    {
        var outbox = GetOrCreateOutbox(clientId);
        await outbox.Lock.WaitAsync(ct);
        try
        {
            outbox.Pending.Add(frame);
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
            outbox.Pending.RemoveAll(f => f.MsgId == msgId);
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
            return [.. outbox.Pending];
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

    private Outbox GetOrCreateOutbox(string clientId) =>
        _outboxes.GetOrAdd(clientId, _ => new Outbox());

    private sealed class Outbox
    {
        public SemaphoreSlim Lock { get; } = new(1, 1);
        public List<Frame> Pending { get; } = [];
    }
}
