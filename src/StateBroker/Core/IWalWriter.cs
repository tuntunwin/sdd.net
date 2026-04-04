using System.Text.Json;

namespace StateBroker.Core;

public interface IWalWriter : IAsyncDisposable
{
    ValueTask<long> AppendAsync(string topic, JsonElement payload, CancellationToken ct);

    ValueTask WaitFlushedAsync(long seq, CancellationToken ct);

    IAsyncEnumerable<WalEntry> ReplayAsync(CancellationToken ct);
}
