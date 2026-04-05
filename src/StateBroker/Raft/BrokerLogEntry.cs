#if ENABLE_RAFT
using DotNext.IO;
using DotNext.Net.Cluster.Consensus.Raft;

namespace StateBroker.Raft;

public readonly struct BrokerLogEntry : IRaftLogEntry
{
    private readonly ReadOnlyMemory<byte> _data;

    public BrokerLogEntry(ReadOnlyMemory<byte> data) => _data = data;

    public long Term { get; init; }
    public DateTimeOffset Timestamp => DateTimeOffset.UtcNow;
    public bool IsSnapshot => false;
    public int? CommandId => null;
    public bool IsReusable => true;
    public long? Length => _data.Length;

    public ValueTask WriteToAsync<TWriter>(TWriter writer, CancellationToken ct)
        where TWriter : notnull, IAsyncBinaryWriter
        => writer.WriteAsync(_data, null, ct);
}
#endif
