using System.Text.Json;
using StateBroker.Core;

namespace StateBroker.Tests;

public class WalWriterTests : IAsyncLifetime
{
    private string _dir = null!;

    public Task InitializeAsync()
    {
        _dir = Path.Combine(Path.GetTempPath(), $"wal-test-{Guid.NewGuid():N}");
        return Task.CompletedTask;
    }

    public Task DisposeAsync()
    {
        if (Directory.Exists(_dir))
            Directory.Delete(_dir, recursive: true);
        return Task.CompletedTask;
    }

    private static JsonElement Json(string raw) =>
        JsonDocument.Parse(raw).RootElement;

    // ── Append + Replay ──

    [Fact]
    public async Task Append_and_replay_round_trip()
    {
        using var cts = new CancellationTokenSource();

        {
            var wal = new WalWriter(_dir, fsyncIntervalMs: 1);
            _ = wal.FlushLoopAsync(cts.Token);

            var s1 = await wal.AppendAsync("a", Json("1"), CancellationToken.None);
            var s2 = await wal.AppendAsync("b", Json("2"), CancellationToken.None);
            await wal.WaitFlushedAsync(s2, CancellationToken.None);

            Assert.Equal(1, s1);
            Assert.Equal(2, s2);

            cts.Cancel();
            await wal.DisposeAsync();
        }

        // Replay from a fresh instance
        {
            var wal2 = new WalWriter(_dir, fsyncIntervalMs: 1);
            var entries = new List<WalEntry>();
            await foreach (var e in wal2.ReplayAsync(CancellationToken.None))
                entries.Add(e);
            await wal2.DisposeAsync();

            Assert.Equal(2, entries.Count);
            Assert.Equal("a", entries[0].Topic);
            Assert.Equal(1, entries[0].Payload.GetInt32());
            Assert.Equal("b", entries[1].Topic);
            Assert.Equal(2, entries[1].Payload.GetInt32());
        }
    }

    [Fact]
    public async Task Monotonic_sequences()
    {
        var wal = new WalWriter(_dir, fsyncIntervalMs: 1);

        var seqs = new List<long>();
        for (var i = 0; i < 20; i++)
            seqs.Add(await wal.AppendAsync($"t{i}", Json($"{i}"), CancellationToken.None));

        await wal.DisposeAsync();

        for (var i = 1; i < seqs.Count; i++)
            Assert.True(seqs[i] > seqs[i - 1]);
    }

    [Fact]
    public async Task WaitFlushed_blocks_until_flush()
    {
        using var cts = new CancellationTokenSource();
        var wal = new WalWriter(_dir, fsyncIntervalMs: 2);
        _ = wal.FlushLoopAsync(cts.Token);

        var seq = await wal.AppendAsync("t", Json("1"), CancellationToken.None);

        var waitTask = wal.WaitFlushedAsync(seq, CancellationToken.None);
        var completed = await Task.WhenAny(waitTask.AsTask(), Task.Delay(1000));
        Assert.True(completed == waitTask.AsTask(), "WaitFlushed should complete within 1s");

        cts.Cancel();
        await wal.DisposeAsync();
    }

    [Fact]
    public async Task Creates_directory_if_missing()
    {
        var nested = Path.Combine(_dir, "sub", "dir");
        var wal = new WalWriter(nested, fsyncIntervalMs: 1);
        Assert.True(Directory.Exists(nested));
        await wal.DisposeAsync();
    }

    // ── Concurrent appends ──

    [Fact]
    public async Task Concurrent_appends_all_persisted()
    {
        using var cts = new CancellationTokenSource();

        {
            var wal = new WalWriter(_dir, fsyncIntervalMs: 1);
            _ = wal.FlushLoopAsync(cts.Token);

            var tasks = Enumerable.Range(0, 50).Select(i =>
                wal.AppendAsync($"t/{i}", Json($"{i}"), CancellationToken.None).AsTask()
            ).ToArray();

            var seqs = await Task.WhenAll(tasks);
            await wal.WaitFlushedAsync(seqs.Max(), CancellationToken.None);

            Assert.Equal(50, seqs.Distinct().Count());

            cts.Cancel();
            await wal.DisposeAsync();
        }

        // Replay and verify
        {
            var wal2 = new WalWriter(_dir, fsyncIntervalMs: 1);
            var entries = new List<WalEntry>();
            await foreach (var e in wal2.ReplayAsync(CancellationToken.None))
                entries.Add(e);
            await wal2.DisposeAsync();

            Assert.Equal(50, entries.Count);
        }
    }

    // ── Segment rotation ──

    [Fact]
    public async Task Segment_rotation_at_threshold()
    {
        using var cts = new CancellationTokenSource();

        {
            // segmentMb=0 means 0 bytes threshold — rotate on every flush
            var wal = new WalWriter(_dir, segmentMb: 0, fsyncIntervalMs: 1);
            _ = wal.FlushLoopAsync(cts.Token);

            for (var i = 0; i < 10; i++)
            {
                var seq = await wal.AppendAsync($"t/{i}", Json($"{i}"), CancellationToken.None);
                await wal.WaitFlushedAsync(seq, CancellationToken.None);
            }

            await Task.Delay(50);
            cts.Cancel();
            await wal.DisposeAsync();
        }

        // Should have segment files
        var segments = Directory.GetFiles(_dir, "wal-*.log");
        Assert.True(segments.Length > 0, "Expected segment files after rotation");

        // All entries replay correctly across segments
        {
            var wal2 = new WalWriter(_dir, fsyncIntervalMs: 1);
            var entries = new List<WalEntry>();
            await foreach (var e in wal2.ReplayAsync(CancellationToken.None))
                entries.Add(e);
            await wal2.DisposeAsync();

            Assert.Equal(10, entries.Count);
            for (var i = 0; i < 10; i++)
                Assert.Equal($"t/{i}", entries[i].Topic);
        }
    }

    // ── Complex payloads ──

    [Fact]
    public async Task Complex_json_payloads()
    {
        using var cts = new CancellationTokenSource();

        {
            var wal = new WalWriter(_dir, fsyncIntervalMs: 1);
            _ = wal.FlushLoopAsync(cts.Token);

            var payload = Json("""{"nested":{"arr":[1,2,3],"flag":true},"name":"test"}""");
            var seq = await wal.AppendAsync("complex", payload, CancellationToken.None);
            await wal.WaitFlushedAsync(seq, CancellationToken.None);

            cts.Cancel();
            await wal.DisposeAsync();
        }

        {
            var wal2 = new WalWriter(_dir, fsyncIntervalMs: 1);
            var entries = new List<WalEntry>();
            await foreach (var e in wal2.ReplayAsync(CancellationToken.None))
                entries.Add(e);
            await wal2.DisposeAsync();

            Assert.Single(entries);
            var p = entries[0].Payload;
            Assert.Equal(3, p.GetProperty("nested").GetProperty("arr").GetArrayLength());
            Assert.True(p.GetProperty("nested").GetProperty("flag").GetBoolean());
        }
    }

    // ── Replay while writer is open ──

    [Fact]
    public async Task Replay_while_writer_is_open()
    {
        var wal = new WalWriter(_dir, fsyncIntervalMs: 1);
        using var cts = new CancellationTokenSource();
        _ = wal.FlushLoopAsync(cts.Token);

        var seq = await wal.AppendAsync("t", Json("1"), CancellationToken.None);
        await wal.WaitFlushedAsync(seq, CancellationToken.None);

        // Replay from the same instance (file is still open for writing)
        var entries = new List<WalEntry>();
        await foreach (var e in wal.ReplayAsync(CancellationToken.None))
            entries.Add(e);

        Assert.Single(entries);
        Assert.Equal("t", entries[0].Topic);

        cts.Cancel();
        await wal.DisposeAsync();
    }

    // ── Empty replay ──

    [Fact]
    public async Task Replay_empty_directory()
    {
        var wal = new WalWriter(_dir, fsyncIntervalMs: 1);

        var entries = new List<WalEntry>();
        await foreach (var e in wal.ReplayAsync(CancellationToken.None))
            entries.Add(e);

        Assert.Empty(entries);
        await wal.DisposeAsync();
    }
}
