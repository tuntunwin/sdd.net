#if ENABLE_RAFT
using System.Buffers;
using System.Text.Json;
using DotNext.Buffers;
using DotNext.IO;
using DotNext.Net.Cluster.Consensus.Raft;
using StateBroker.Core;

namespace StateBroker.Raft;

/// <summary>
/// Raft state machine backed by in-memory StateStore. Log on disk via DotNext.
/// ApplyAsync is called on every node after quorum commit.
/// </summary>
public sealed class BrokerStateMachine : MemoryBasedStateMachine
{
    private readonly IStateStore _store;
    private readonly ISubscriberNotifier _notifier;

    public BrokerStateMachine(
        string dataDir,
        IStateStore store,
        ISubscriberNotifier notifier)
        : base(dataDir, recordsPerPartition: 1024,
               new Options { CompactionMode = CompactionMode.Background })
    {
        _store = store;
        _notifier = notifier;
    }

    protected override async ValueTask ApplyAsync(LogEntry entry)
    {
        if (entry.IsSnapshot)
        {
            await RestoreSnapshotAsync(entry);
            return;
        }

        using var memory = await entry.ToMemoryAsync(allocator: null, CancellationToken.None);
        if (memory.Length == 0) return;

        var op = JsonSerializer.Deserialize(
            memory.Span, BrokerJsonContext.Default.StateEntry);
        if (op is null) return;

        switch (op.Op)
        {
            case StateOp.Set:
                _store.SetMemory(op.Topic, op.Payload);
                _notifier.NotifySet(op.Topic, op.Payload);
                break;
            case StateOp.Delete:
                _store.DeleteMemory(op.Topic);
                _notifier.NotifyDelete(op.Topic);
                break;
        }
    }

    protected override SnapshotBuilder CreateSnapshotBuilder(
        in SnapshotBuilderContext context)
    {
        return new BrokerSnapshotBuilder(context, _store);
    }

    private async ValueTask RestoreSnapshotAsync(LogEntry snapshot)
    {
        using var memory = await snapshot.ToMemoryAsync(allocator: null, CancellationToken.None);
        if (memory.Length == 0) return;

        var entries = JsonSerializer.Deserialize(
            memory.Span, BrokerJsonContext.Default.StateEntryArray);
        if (entries is not null)
            _store.LoadBulk(entries);
    }

    private sealed class BrokerSnapshotBuilder : IncrementalSnapshotBuilder
    {
        private readonly IStateStore _store;

        public BrokerSnapshotBuilder(
            in SnapshotBuilderContext context, IStateStore store)
            : base(context)
        {
            _store = store;
        }

        // State is already in _store — no need to re-accumulate from log entries
        protected override ValueTask ApplyAsync(LogEntry entry) =>
            ValueTask.CompletedTask;

        public override async ValueTask WriteToAsync<TWriter>(
            TWriter writer, CancellationToken ct)
        {
            var buf = new ArrayBufferWriter<byte>();
            using (var jsonWriter = new Utf8JsonWriter(buf))
            {
                jsonWriter.WriteStartArray();
                foreach (var (topic, payload) in _store.GetAll())
                {
                    jsonWriter.WriteStartObject();
                    jsonWriter.WriteString("topic", topic);
                    jsonWriter.WritePropertyName("payload");
                    payload.WriteTo(jsonWriter);
                    jsonWriter.WriteNumber("op", (int)StateOp.Set);
                    jsonWriter.WriteEndObject();
                }
                jsonWriter.WriteEndArray();
            }
            await writer.WriteAsync(buf.WrittenMemory, null, ct);
        }
    }
}
#endif
