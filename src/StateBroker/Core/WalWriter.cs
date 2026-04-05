using System.Buffers;
using System.Runtime.CompilerServices;
using System.Text.Json;

namespace StateBroker.Core;

public sealed class WalWriter : IWalWriter
{
    private readonly string _dir;
    private readonly string _activePath;
    private readonly long _segmentMaxBytes;
    private readonly int _fsyncIntervalMs;

    private readonly SemaphoreSlim _appendLock = new(1, 1);
    private readonly SemaphoreSlim _flushGate = new(0);

    private long _seq;
    private long _writtenSeq;
    private long _flushedSeq;
    private int _waitingWriters;
    private int _segmentCounter;

    private FileStream _file;

    public WalWriter(string dir, int segmentMb = 64, int fsyncIntervalMs = 2)
    {
        _dir = dir;
        _segmentMaxBytes = (long)segmentMb * 1024 * 1024;
        _fsyncIntervalMs = fsyncIntervalMs;
        _activePath = Path.Combine(dir, "wal.log");

        Directory.CreateDirectory(dir);
        _file = OpenActiveFile();
    }

    public async ValueTask<long> AppendAsync(
        string topic, JsonElement payload, CancellationToken ct)
    {
        var seq = Interlocked.Increment(ref _seq);

        var buf = new ArrayBufferWriter<byte>();
        using (var writer = new Utf8JsonWriter(buf))
        {
            writer.WriteStartObject();
            writer.WriteNumber("seq", seq);
            writer.WriteString("topic", topic);
            writer.WritePropertyName("payload");
            payload.WriteTo(writer);
            writer.WriteEndObject();
        }
        buf.Write("\n"u8);

        await _appendLock.WaitAsync(ct);
        try
        {
            await _file.WriteAsync(buf.WrittenMemory, ct);
            Volatile.Write(ref _writtenSeq, seq);
        }
        finally
        {
            _appendLock.Release();
        }

        return seq;
    }

    public async ValueTask<long> AppendDeleteAsync(string topic, CancellationToken ct)
    {
        var seq = Interlocked.Increment(ref _seq);

        var buf = new ArrayBufferWriter<byte>();
        using (var writer = new Utf8JsonWriter(buf))
        {
            writer.WriteStartObject();
            writer.WriteNumber("seq", seq);
            writer.WriteString("topic", topic);
            writer.WriteBoolean("delete", true);
            writer.WriteEndObject();
        }
        buf.Write("\n"u8);

        await _appendLock.WaitAsync(ct);
        try
        {
            await _file.WriteAsync(buf.WrittenMemory, ct);
            Volatile.Write(ref _writtenSeq, seq);
        }
        finally
        {
            _appendLock.Release();
        }

        return seq;
    }

    public ValueTask WaitFlushedAsync(long seq, CancellationToken ct)
    {
        if (Volatile.Read(ref _flushedSeq) >= seq)
            return ValueTask.CompletedTask;

        Interlocked.Increment(ref _waitingWriters);
        return new ValueTask(_flushGate.WaitAsync(ct));
    }

    public async Task FlushLoopAsync(CancellationToken ct)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(_fsyncIntervalMs));
        while (await timer.WaitForNextTickAsync(ct))
        {
            var written = Volatile.Read(ref _writtenSeq);
            if (written <= Volatile.Read(ref _flushedSeq))
                continue;

            _file.Flush(flushToDisk: true);
            Volatile.Write(ref _flushedSeq, written);

            var waiting = Interlocked.Exchange(ref _waitingWriters, 0);
            if (waiting > 0)
                _flushGate.Release(waiting);

            if (_file.Length >= _segmentMaxBytes)
                await RotateSegmentAsync();
        }
    }

    public async IAsyncEnumerable<WalEntry> ReplayAsync(
        [EnumeratorCancellation] CancellationToken ct)
    {
        // Read sealed segments in order, then active file
        var segments = Directory.Exists(_dir)
            ? Directory.GetFiles(_dir, "wal-*.log")
            : [];
        Array.Sort(segments, StringComparer.Ordinal);

        foreach (var path in segments)
        {
            await foreach (var entry in ReadFileAsync(path, ct))
                yield return entry;
        }

        if (File.Exists(_activePath))
        {
            await foreach (var entry in ReadFileAsync(_activePath, ct))
                yield return entry;
        }
    }

    public async ValueTask DisposeAsync()
    {
        await _appendLock.WaitAsync(CancellationToken.None);
        try
        {
            _file.Flush(flushToDisk: true);
            await _file.DisposeAsync();
        }
        finally
        {
            _appendLock.Release();
        }
        _appendLock.Dispose();
        _flushGate.Dispose();
    }

    private async Task RotateSegmentAsync()
    {
        await _appendLock.WaitAsync(CancellationToken.None);
        try
        {
            _file.Flush(flushToDisk: true);
            await _file.DisposeAsync();

            var segNum = Interlocked.Increment(ref _segmentCounter);
            var segPath = Path.Combine(_dir, $"wal-{segNum:D8}.log");
            File.Move(_activePath, segPath);

            _file = OpenActiveFile();
        }
        finally
        {
            _appendLock.Release();
        }
    }

    private FileStream OpenActiveFile() =>
        new(_activePath,
            FileMode.Append,
            FileAccess.Write,
            FileShare.Read,
            bufferSize: 65536,
            FileOptions.Asynchronous | FileOptions.SequentialScan);

    private static async IAsyncEnumerable<WalEntry> ReadFileAsync(
        string path, [EnumeratorCancellation] CancellationToken ct)
    {
        using var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        using var reader = new StreamReader(fs);
        while (await reader.ReadLineAsync(ct) is { } line)
        {
            if (string.IsNullOrWhiteSpace(line))
                continue;

            WalEntry? entry;
            try
            {
                entry = JsonSerializer.Deserialize(line, BrokerJsonContext.Default.WalEntry);
            }
            catch (JsonException)
            {
                continue; // skip corrupt lines
            }

            if (entry is not null)
                yield return entry;
        }
    }
}
