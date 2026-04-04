using System.Runtime.CompilerServices;
using System.Text.Json;

namespace StateBroker.Core;

public sealed class NullWalWriter : IWalWriter
{
    private long _seq;

    public ValueTask<long> AppendAsync(string topic, JsonElement payload, CancellationToken ct) =>
        ValueTask.FromResult(Interlocked.Increment(ref _seq));

    public ValueTask WaitFlushedAsync(long seq, CancellationToken ct) =>
        ValueTask.CompletedTask;

    public async IAsyncEnumerable<WalEntry> ReplayAsync(
        [EnumeratorCancellation] CancellationToken ct)
    {
        await Task.CompletedTask;
        yield break;
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
