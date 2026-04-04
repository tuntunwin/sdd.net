using System.Text.Json;
using StateBroker.Core;

namespace StateBroker.Tests;

public class ShardedStateStoreTests : IDisposable
{
    private readonly ShardedStateStore _store = new(shardCount: 64);

    public void Dispose() => _store.Dispose();

    private static JsonElement Json(string raw) =>
        JsonDocument.Parse(raw).RootElement;

    // ── Construction ──

    [Fact]
    public void Default_construction()
    {
        using var store = new ShardedStateStore();
        Assert.Null(store.Get("any"));
        Assert.Empty(store.GetAll());
    }

    [Theory]
    [InlineData(0)]
    [InlineData(3)]
    [InlineData(5)]
    [InlineData(7)]
    [InlineData(-1)]
    public void Rejects_non_power_of_two_shard_count(int shards)
    {
        Assert.Throws<ArgumentException>(() => new ShardedStateStore(shards));
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(4)]
    [InlineData(32)]
    [InlineData(64)]
    [InlineData(128)]
    public void Accepts_power_of_two_shard_count(int shards)
    {
        using var store = new ShardedStateStore(shards);
        Assert.NotNull(store);
    }

    // ── SetMemory / Get ──

    [Fact]
    public void SetMemory_and_Get()
    {
        _store.SetMemory("sensors/temp", Json("22.5"));
        var result = _store.Get("sensors/temp");

        Assert.NotNull(result);
        Assert.Equal(22.5, result.Value.GetDouble());
    }

    [Fact]
    public void Get_missing_returns_null()
    {
        Assert.Null(_store.Get("nonexistent"));
    }

    [Fact]
    public void SetMemory_overwrites_existing()
    {
        _store.SetMemory("t", Json("1"));
        _store.SetMemory("t", Json("2"));

        Assert.Equal(2, _store.Get("t")!.Value.GetInt32());
    }

    [Fact]
    public void SetMemory_different_topics()
    {
        _store.SetMemory("a", Json("1"));
        _store.SetMemory("b", Json("2"));
        _store.SetMemory("c", Json("3"));

        Assert.Equal(1, _store.Get("a")!.Value.GetInt32());
        Assert.Equal(2, _store.Get("b")!.Value.GetInt32());
        Assert.Equal(3, _store.Get("c")!.Value.GetInt32());
    }

    [Fact]
    public void SetMemory_with_complex_payload()
    {
        var payload = Json("""{"nested":{"arr":[1,2,3],"flag":true}}""");
        _store.SetMemory("complex", payload);

        var result = _store.Get("complex")!.Value;
        Assert.Equal(3, result.GetProperty("nested").GetProperty("arr").GetArrayLength());
        Assert.True(result.GetProperty("nested").GetProperty("flag").GetBoolean());
    }

    // ── DeleteMemory ──

    [Fact]
    public void DeleteMemory_removes_topic()
    {
        _store.SetMemory("t", Json("1"));
        _store.DeleteMemory("t");

        Assert.Null(_store.Get("t"));
    }

    [Fact]
    public void DeleteMemory_nonexistent_is_noop()
    {
        _store.DeleteMemory("nonexistent"); // should not throw
    }

    [Fact]
    public void DeleteMemory_only_affects_target()
    {
        _store.SetMemory("a", Json("1"));
        _store.SetMemory("b", Json("2"));
        _store.DeleteMemory("a");

        Assert.Null(_store.Get("a"));
        Assert.Equal(2, _store.Get("b")!.Value.GetInt32());
    }

    // ── GetAll ──

    [Fact]
    public void GetAll_empty()
    {
        Assert.Empty(_store.GetAll());
    }

    [Fact]
    public void GetAll_returns_all_entries()
    {
        _store.SetMemory("x", Json("10"));
        _store.SetMemory("y", Json("20"));
        _store.SetMemory("z", Json("30"));

        var all = _store.GetAll().OrderBy(e => e.Topic).ToList();
        Assert.Equal(3, all.Count);
        Assert.Equal("x", all[0].Topic);
        Assert.Equal(10, all[0].Payload.GetInt32());
        Assert.Equal("y", all[1].Topic);
        Assert.Equal("z", all[2].Topic);
    }

    [Fact]
    public void GetAll_reflects_deletes()
    {
        _store.SetMemory("a", Json("1"));
        _store.SetMemory("b", Json("2"));
        _store.DeleteMemory("a");

        var all = _store.GetAll().ToList();
        Assert.Single(all);
        Assert.Equal("b", all[0].Topic);
    }

    // ── SetAsync (WAL integration) ──

    [Fact]
    public async Task SetAsync_writes_through_wal_then_memory()
    {
        var wal = new TrackingWalWriter();
        using var store = new ShardedStateStore(wal: wal);

        await store.SetAsync("t", Json("42"), CancellationToken.None);

        Assert.Equal(42, store.Get("t")!.Value.GetInt32());
        Assert.Single(wal.Appends);
        Assert.Equal("t", wal.Appends[0].Topic);
        Assert.Equal(1, wal.FlushedSeqs.Count);
    }

    [Fact]
    public async Task SetAsync_multiple_topics()
    {
        var wal = new TrackingWalWriter();
        using var store = new ShardedStateStore(wal: wal);

        await store.SetAsync("a", Json("1"), CancellationToken.None);
        await store.SetAsync("b", Json("2"), CancellationToken.None);

        Assert.Equal(1, store.Get("a")!.Value.GetInt32());
        Assert.Equal(2, store.Get("b")!.Value.GetInt32());
        Assert.Equal(2, wal.Appends.Count);
    }

    [Fact]
    public async Task ConfigureWal_swaps_wal()
    {
        var wal1 = new TrackingWalWriter();
        var wal2 = new TrackingWalWriter();
        using var store = new ShardedStateStore(wal: wal1);

        await store.SetAsync("a", Json("1"), CancellationToken.None);
        store.ConfigureWal(wal2);
        await store.SetAsync("b", Json("2"), CancellationToken.None);

        Assert.Single(wal1.Appends);
        Assert.Single(wal2.Appends);
    }

    // ── LoadBulk ──

    [Fact]
    public void LoadBulk_replaces_all_state()
    {
        _store.SetMemory("old1", Json("1"));
        _store.SetMemory("old2", Json("2"));

        _store.LoadBulk([
            new StateEntry("new1", Json("10"), StateOp.Set),
            new StateEntry("new2", Json("20"), StateOp.Set),
        ]);

        Assert.Null(_store.Get("old1"));
        Assert.Null(_store.Get("old2"));
        Assert.Equal(10, _store.Get("new1")!.Value.GetInt32());
        Assert.Equal(20, _store.Get("new2")!.Value.GetInt32());
    }

    [Fact]
    public void LoadBulk_empty_clears_all()
    {
        _store.SetMemory("a", Json("1"));
        _store.LoadBulk([]);

        Assert.Empty(_store.GetAll());
    }

    [Fact]
    public void LoadBulk_skips_delete_ops()
    {
        _store.LoadBulk([
            new StateEntry("a", Json("1"), StateOp.Set),
            new StateEntry("b", Json("2"), StateOp.Delete),
        ]);

        Assert.Equal(1, _store.Get("a")!.Value.GetInt32());
        Assert.Null(_store.Get("b"));
    }

    // ── Sharding distribution ──

    [Fact]
    public void Topics_distribute_across_shards()
    {
        // Write enough topics that they can't all land in the same shard
        for (var i = 0; i < 256; i++)
            _store.SetMemory($"topic/{i}", Json($"{i}"));

        var all = _store.GetAll().ToList();
        Assert.Equal(256, all.Count);

        // Verify all readable back
        for (var i = 0; i < 256; i++)
            Assert.Equal(i, _store.Get($"topic/{i}")!.Value.GetInt32());
    }

    // ── Concurrency ──

    [Fact]
    public async Task Concurrent_reads_and_writes()
    {
        const int writerCount = 10;
        const int writesPerWriter = 100;
        const int readerCount = 10;
        const int readsPerReader = 200;

        using var cts = new CancellationTokenSource();

        // Seed some initial data
        for (var i = 0; i < 50; i++)
            _store.SetMemory($"t/{i}", Json($"{i}"));

        var writers = Enumerable.Range(0, writerCount).Select(w => Task.Run(() =>
        {
            for (var i = 0; i < writesPerWriter; i++)
            {
                var topic = $"t/{(w * writesPerWriter + i) % 200}";
                _store.SetMemory(topic, Json($"{w * writesPerWriter + i}"));
            }
        })).ToArray();

        var readers = Enumerable.Range(0, readerCount).Select(_ => Task.Run(() =>
        {
            for (var i = 0; i < readsPerReader; i++)
            {
                _store.Get($"t/{i % 200}"); // may be null, that's fine
            }
        })).ToArray();

        await Task.WhenAll(writers.Concat(readers));

        // Store should be consistent — no exceptions, no corruption
        var all = _store.GetAll().ToList();
        Assert.True(all.Count > 0);
    }

    [Fact]
    public async Task Concurrent_SetAsync_produces_no_lost_updates()
    {
        var wal = new TrackingWalWriter();
        using var store = new ShardedStateStore(wal: wal);

        // Write 100 distinct topics concurrently
        var tasks = Enumerable.Range(0, 100).Select(i =>
            store.SetAsync($"topic/{i}", Json($"{i}"), CancellationToken.None).AsTask()
        ).ToArray();

        await Task.WhenAll(tasks);

        Assert.Equal(100, wal.Appends.Count);
        for (var i = 0; i < 100; i++)
            Assert.Equal(i, store.Get($"topic/{i}")!.Value.GetInt32());
    }

    [Fact]
    public async Task Concurrent_deletes_and_sets()
    {
        // Pre-populate
        for (var i = 0; i < 100; i++)
            _store.SetMemory($"t/{i}", Json($"{i}"));

        var setters = Task.Run(() =>
        {
            for (var i = 0; i < 100; i++)
                _store.SetMemory($"t/{i}", Json($"{i + 1000}"));
        });

        var deleters = Task.Run(() =>
        {
            for (var i = 50; i < 100; i++)
                _store.DeleteMemory($"t/{i}");
        });

        await Task.WhenAll(setters, deleters);

        // First 50 should still exist (may have old or new value depending on ordering)
        for (var i = 0; i < 50; i++)
            Assert.NotNull(_store.Get($"t/{i}"));
    }

    // ── Small shard count ──

    [Fact]
    public void Single_shard_works()
    {
        using var store = new ShardedStateStore(shardCount: 1);
        store.SetMemory("a", Json("1"));
        store.SetMemory("b", Json("2"));

        Assert.Equal(1, store.Get("a")!.Value.GetInt32());
        Assert.Equal(2, store.Get("b")!.Value.GetInt32());
        Assert.Equal(2, store.GetAll().Count());
    }

    // ── Helper ──

    private sealed class TrackingWalWriter : IWalWriter
    {
        private long _seq;
        public List<(string Topic, JsonElement Payload)> Appends { get; } = [];
        public List<long> FlushedSeqs { get; } = [];

        public ValueTask<long> AppendAsync(string topic, JsonElement payload, CancellationToken ct)
        {
            lock (Appends)
            {
                Appends.Add((topic, payload));
            }
            return ValueTask.FromResult(Interlocked.Increment(ref _seq));
        }

        public ValueTask WaitFlushedAsync(long seq, CancellationToken ct)
        {
            lock (FlushedSeqs)
            {
                FlushedSeqs.Add(seq);
            }
            return ValueTask.CompletedTask;
        }

        public async IAsyncEnumerable<WalEntry> ReplayAsync(
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct)
        {
            await Task.CompletedTask;
            yield break;
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
