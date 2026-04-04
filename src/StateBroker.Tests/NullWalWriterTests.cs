using System.Text.Json;
using StateBroker.Core;

namespace StateBroker.Tests;

public class NullWalWriterTests
{
    [Fact]
    public async Task Append_returns_monotonic_sequence()
    {
        await using var wal = new NullWalWriter();
        var payload = JsonDocument.Parse("1").RootElement;

        var s1 = await wal.AppendAsync("a", payload, CancellationToken.None);
        var s2 = await wal.AppendAsync("b", payload, CancellationToken.None);
        var s3 = await wal.AppendAsync("c", payload, CancellationToken.None);

        Assert.Equal(1, s1);
        Assert.Equal(2, s2);
        Assert.Equal(3, s3);
    }

    [Fact]
    public async Task WaitFlushed_completes_immediately()
    {
        await using var wal = new NullWalWriter();

        // Should not block or throw
        await wal.WaitFlushedAsync(999, CancellationToken.None);
    }

    [Fact]
    public async Task Replay_yields_nothing()
    {
        await using var wal = new NullWalWriter();
        var payload = JsonDocument.Parse("1").RootElement;

        // Append some entries
        await wal.AppendAsync("a", payload, CancellationToken.None);
        await wal.AppendAsync("b", payload, CancellationToken.None);

        // Replay should still be empty (NullWal stores nothing)
        var entries = new List<WalEntry>();
        await foreach (var e in wal.ReplayAsync(CancellationToken.None))
            entries.Add(e);

        Assert.Empty(entries);
    }

    [Fact]
    public async Task Dispose_is_idempotent()
    {
        var wal = new NullWalWriter();
        await wal.DisposeAsync();
        await wal.DisposeAsync(); // no throw
    }

    [Fact]
    public async Task Concurrent_appends_produce_unique_sequences()
    {
        await using var wal = new NullWalWriter();
        var payload = JsonDocument.Parse("1").RootElement;

        var tasks = Enumerable.Range(0, 100)
            .Select(_ => wal.AppendAsync("t", payload, CancellationToken.None).AsTask())
            .ToArray();

        var results = await Task.WhenAll(tasks);
        var distinct = results.Distinct().ToList();

        Assert.Equal(100, distinct.Count);
        Assert.Equal(1, distinct.Min());
        Assert.Equal(100, distinct.Max());
    }
}
