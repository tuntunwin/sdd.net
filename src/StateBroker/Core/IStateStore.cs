using System.Text.Json;

namespace StateBroker.Core;

public interface IStateStore
{
    ValueTask SetAsync(string topic, JsonElement payload, CancellationToken ct);

    /// <summary>
    /// In-memory-only set, bypassing WAL. Used by Raft ApplyAsync.
    /// </summary>
    void SetMemory(string topic, JsonElement payload);

    void DeleteMemory(string topic);

    JsonElement? Get(string topic);

    IEnumerable<(string Topic, JsonElement Payload)> GetAll();

    /// <summary>
    /// Replace all shard contents atomically. Used for snapshot restore.
    /// </summary>
    void LoadBulk(IEnumerable<StateEntry> entries);
}
