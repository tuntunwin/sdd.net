using System.Text.Json;

namespace StateBroker.Core;

public sealed class ShardedStateStore : IStateStore, IDisposable
{
    private readonly int _shardCount;
    private readonly Shard[] _shards;
    private IWalWriter _wal;

    public ShardedStateStore(int shardCount = 64, IWalWriter? wal = null)
    {
        if (shardCount <= 0 || (shardCount & (shardCount - 1)) != 0)
            throw new ArgumentException("Shard count must be a positive power of 2.", nameof(shardCount));

        _shardCount = shardCount;
        _shards = new Shard[shardCount];
        for (var i = 0; i < shardCount; i++)
            _shards[i] = new Shard();

        _wal = wal ?? new NullWalWriter();
    }

    public void ConfigureWal(IWalWriter wal) => _wal = wal;

    public async ValueTask SetAsync(string topic, JsonElement payload, CancellationToken ct)
    {
        // WAL first — durable before memory
        var seq = await _wal.AppendAsync(topic, payload, ct);
        await _wal.WaitFlushedAsync(seq, ct);

        // Memory update — lock held only for dictionary op, never across await
        SetMemory(topic, payload);
    }

    public void SetMemory(string topic, JsonElement payload)
    {
        var shard = Pick(topic);
        shard.Lock.EnterWriteLock();
        try
        {
            shard.Data[topic] = payload;
        }
        finally
        {
            shard.Lock.ExitWriteLock();
        }
    }

    public void DeleteMemory(string topic)
    {
        var shard = Pick(topic);
        shard.Lock.EnterWriteLock();
        try
        {
            shard.Data.Remove(topic);
        }
        finally
        {
            shard.Lock.ExitWriteLock();
        }
    }

    public JsonElement? Get(string topic)
    {
        var shard = Pick(topic);
        shard.Lock.EnterReadLock();
        try
        {
            return shard.Data.TryGetValue(topic, out var v) ? v : null;
        }
        finally
        {
            shard.Lock.ExitReadLock();
        }
    }

    public IEnumerable<(string Topic, JsonElement Payload)> GetAll()
    {
        var results = new List<(string, JsonElement)>();
        for (var i = 0; i < _shardCount; i++)
        {
            var shard = _shards[i];
            shard.Lock.EnterReadLock();
            try
            {
                foreach (var kvp in shard.Data)
                    results.Add((kvp.Key, kvp.Value));
            }
            finally
            {
                shard.Lock.ExitReadLock();
            }
        }
        return results;
    }

    public void LoadBulk(IEnumerable<StateEntry> entries)
    {
        // Clear all shards under write locks
        for (var i = 0; i < _shardCount; i++)
        {
            _shards[i].Lock.EnterWriteLock();
        }
        try
        {
            for (var i = 0; i < _shardCount; i++)
                _shards[i].Data.Clear();

            foreach (var entry in entries)
            {
                if (entry.Op == StateOp.Set)
                {
                    var shard = Pick(entry.Topic);
                    shard.Data[entry.Topic] = entry.Payload;
                }
            }
        }
        finally
        {
            for (var i = _shardCount - 1; i >= 0; i--)
                _shards[i].Lock.ExitWriteLock();
        }
    }

    private Shard Pick(string topic) =>
        _shards[(uint)HashCode.Combine(topic) % (uint)_shardCount];

    public void Dispose()
    {
        for (var i = 0; i < _shardCount; i++)
            _shards[i].Lock.Dispose();
    }

    private sealed class Shard
    {
        public readonly ReaderWriterLockSlim Lock = new(LockRecursionPolicy.NoRecursion);
        public readonly Dictionary<string, JsonElement> Data = new();
    }
}
