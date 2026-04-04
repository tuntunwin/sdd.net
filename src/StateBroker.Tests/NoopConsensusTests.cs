using StateBroker.Core;

namespace StateBroker.Tests;

public class NoopConsensusTests
{
    [Fact]
    public void IsLeader_always_true()
    {
        var consensus = new NoopConsensus();
        Assert.True(consensus.IsLeader);
    }

    [Fact]
    public async Task ProposeAsync_completes_immediately()
    {
        var consensus = new NoopConsensus();
        var data = new byte[] { 1, 2, 3 };

        // Should not block or throw
        await consensus.ProposeAsync(data, CancellationToken.None);
    }

    [Fact]
    public async Task ProposeAsync_with_empty_data()
    {
        var consensus = new NoopConsensus();
        await consensus.ProposeAsync(ReadOnlyMemory<byte>.Empty, CancellationToken.None);
    }
}
