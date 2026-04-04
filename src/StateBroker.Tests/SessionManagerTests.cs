using StateBroker.Core;

namespace StateBroker.Tests;

public class SessionManagerTests
{
    private readonly SessionManager _mgr = new(sendBufferDepth: 16);

    [Fact]
    public void GetOrCreate_creates_new_session()
    {
        var session = _mgr.GetOrCreate("c1");
        Assert.Equal("c1", session.ClientId);
        Assert.Equal(1, _mgr.Count);
    }

    [Fact]
    public void GetOrCreate_returns_same_session()
    {
        var s1 = _mgr.GetOrCreate("c1");
        var s2 = _mgr.GetOrCreate("c1");
        Assert.Same(s1, s2);
        Assert.Equal(1, _mgr.Count);
    }

    [Fact]
    public void GetOrCreate_different_clients()
    {
        var s1 = _mgr.GetOrCreate("c1");
        var s2 = _mgr.GetOrCreate("c2");
        Assert.NotSame(s1, s2);
        Assert.Equal(2, _mgr.Count);
    }

    [Fact]
    public void TryGet_existing()
    {
        _mgr.GetOrCreate("c1");
        Assert.True(_mgr.TryGet("c1", out var session));
        Assert.NotNull(session);
        Assert.Equal("c1", session!.ClientId);
    }

    [Fact]
    public void TryGet_missing()
    {
        Assert.False(_mgr.TryGet("nope", out var session));
        Assert.Null(session);
    }

    [Fact]
    public void TrySend_to_existing()
    {
        var session = _mgr.GetOrCreate("c1");
        var frame = new Frame(Frame.Types.Deliver, "t/1");

        Assert.True(_mgr.TrySend("c1", frame));
        Assert.True(session.SendChannel.Reader.TryRead(out var read));
        Assert.Equal(frame, read);
    }

    [Fact]
    public void TrySend_to_missing_returns_false()
    {
        Assert.False(_mgr.TrySend("nope", new Frame(Frame.Types.Ping)));
    }

    [Fact]
    public void Remove_returns_session()
    {
        _mgr.GetOrCreate("c1");
        var removed = _mgr.Remove("c1");

        Assert.NotNull(removed);
        Assert.Equal("c1", removed!.ClientId);
        Assert.Equal(0, _mgr.Count);
    }

    [Fact]
    public void Remove_missing_returns_null()
    {
        Assert.Null(_mgr.Remove("nope"));
    }

    [Fact]
    public void Remove_then_GetOrCreate_gives_new_session()
    {
        var s1 = _mgr.GetOrCreate("c1");
        _mgr.Remove("c1");
        var s2 = _mgr.GetOrCreate("c1");

        Assert.NotSame(s1, s2);
    }

    [Fact]
    public void ConnectedClientIds_reflects_state()
    {
        _mgr.GetOrCreate("a");
        _mgr.GetOrCreate("b");
        _mgr.GetOrCreate("c");
        _mgr.Remove("b");

        var ids = _mgr.ConnectedClientIds;
        Assert.Equal(2, ids.Count);
        Assert.Contains("a", ids);
        Assert.Contains("c", ids);
    }

    [Fact]
    public void Count_starts_at_zero()
    {
        var mgr = new SessionManager();
        Assert.Equal(0, mgr.Count);
    }

    [Fact]
    public async Task Concurrent_GetOrCreate()
    {
        var tasks = Enumerable.Range(0, 100).Select(i =>
            Task.Run(() => _mgr.GetOrCreate($"c{i % 20}"))
        ).ToArray();

        await Task.WhenAll(tasks);
        Assert.Equal(20, _mgr.Count);
    }
}
