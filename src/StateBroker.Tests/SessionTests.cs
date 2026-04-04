using StateBroker.Core;

namespace StateBroker.Tests;

public class SessionTests : IDisposable
{
    private readonly Session _session = new("client-1", sendBufferDepth: 4);

    public void Dispose() => _session.Dispose();

    [Fact]
    public void ClientId_is_set()
    {
        Assert.Equal("client-1", _session.ClientId);
    }

    [Fact]
    public void TrySend_enqueues_frame()
    {
        var frame = new Frame(Frame.Types.Deliver, "t/1");
        Assert.True(_session.TrySend(frame));
        Assert.True(_session.SendChannel.Reader.TryRead(out var read));
        Assert.Equal(frame, read);
    }

    [Fact]
    public void TrySend_drops_oldest_on_overflow()
    {
        // Buffer depth is 4 — fill it then overflow
        for (var i = 0; i < 4; i++)
            Assert.True(_session.TrySend(new Frame(Frame.Types.Deliver, $"t/{i}")));

        // This should succeed (DropOldest) and push out t/0
        Assert.True(_session.TrySend(new Frame(Frame.Types.Deliver, "t/4")));

        // Drain and verify oldest was dropped
        var topics = new List<string?>();
        while (_session.SendChannel.Reader.TryRead(out var f))
            topics.Add(f.Topic);

        Assert.Equal(4, topics.Count);
        Assert.DoesNotContain("t/0", topics);
        Assert.Contains("t/4", topics);
    }

    [Fact]
    public async Task SendChannel_reader_completes_on_dispose()
    {
        _session.TrySend(new Frame(Frame.Types.Ping));
        _session.Dispose();

        // Reader should drain remaining then complete
        var count = 0;
        await foreach (var _ in _session.SendChannel.Reader.ReadAllAsync())
            count++;

        Assert.Equal(1, count);
    }

    [Fact]
    public void Cancel_sets_cancellation_token()
    {
        Assert.False(_session.CancellationToken.IsCancellationRequested);
        _session.Cancel();
        Assert.True(_session.CancellationToken.IsCancellationRequested);
    }

    [Fact]
    public void Cancel_is_idempotent()
    {
        _session.Cancel();
        _session.Cancel(); // no throw
        Assert.True(_session.CancellationToken.IsCancellationRequested);
    }

    [Fact]
    public void Default_buffer_depth()
    {
        using var session = new Session("c");
        // Should accept 256 frames without dropping
        for (var i = 0; i < 256; i++)
            Assert.True(session.TrySend(new Frame(Frame.Types.Deliver, $"t/{i}")));
    }
}
