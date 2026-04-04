using StateBroker.Core;

namespace StateBroker.Tests;

public class TopicRouterTests : IDisposable
{
    private readonly TopicRouter _router = new();

    public void Dispose() => _router.Dispose();

    // ── Exact match ──

    [Fact]
    public void Exact_match()
    {
        _router.Subscribe("c1", "sensors/temp");
        var result = _router.Match("sensors/temp");
        Assert.Single(result);
        Assert.Equal("c1", result[0]);
    }

    [Fact]
    public void No_match()
    {
        _router.Subscribe("c1", "sensors/temp");
        Assert.Empty(_router.Match("sensors/humidity"));
    }

    [Fact]
    public void Multiple_subscribers_same_topic()
    {
        _router.Subscribe("c1", "sensors/temp");
        _router.Subscribe("c2", "sensors/temp");
        var result = _router.Match("sensors/temp");
        Assert.Equal(2, result.Count);
        Assert.Contains("c1", result);
        Assert.Contains("c2", result);
    }

    [Fact]
    public void Multiple_topics()
    {
        _router.Subscribe("c1", "a/b");
        _router.Subscribe("c2", "x/y");
        Assert.Single(_router.Match("a/b"));
        Assert.Single(_router.Match("x/y"));
        Assert.Empty(_router.Match("a/y"));
    }

    // ── + wildcard ──

    [Fact]
    public void Plus_wildcard_single_level()
    {
        _router.Subscribe("c1", "sensors/+/temp");
        Assert.Single(_router.Match("sensors/living/temp"));
        Assert.Single(_router.Match("sensors/kitchen/temp"));
        Assert.Empty(_router.Match("sensors/temp"));
        Assert.Empty(_router.Match("sensors/living/humidity"));
    }

    [Fact]
    public void Plus_wildcard_at_start()
    {
        _router.Subscribe("c1", "+/temp");
        Assert.Single(_router.Match("sensors/temp"));
        Assert.Single(_router.Match("devices/temp"));
        Assert.Empty(_router.Match("sensors/a/temp"));
    }

    [Fact]
    public void Plus_wildcard_at_end()
    {
        _router.Subscribe("c1", "sensors/+");
        Assert.Single(_router.Match("sensors/temp"));
        Assert.Single(_router.Match("sensors/humidity"));
        Assert.Empty(_router.Match("sensors/a/b"));
    }

    [Fact]
    public void Multiple_plus_wildcards()
    {
        _router.Subscribe("c1", "+/+/temp");
        Assert.Single(_router.Match("a/b/temp"));
        Assert.Empty(_router.Match("a/temp"));
        Assert.Empty(_router.Match("a/b/c/temp"));
    }

    // ── # wildcard ──

    [Fact]
    public void Hash_wildcard_matches_all_below()
    {
        _router.Subscribe("c1", "sensors/#");
        Assert.Single(_router.Match("sensors/temp"));
        Assert.Single(_router.Match("sensors/a/b/c"));
        Assert.Single(_router.Match("sensors/temp/living"));
    }

    [Fact]
    public void Hash_wildcard_at_root()
    {
        _router.Subscribe("c1", "#");
        Assert.Single(_router.Match("anything"));
        Assert.Single(_router.Match("a/b/c"));
    }

    [Fact]
    public void Hash_wildcard_no_partial_level()
    {
        _router.Subscribe("c1", "sensors/#");
        Assert.Empty(_router.Match("sensor"));
        Assert.Empty(_router.Match("other/temp"));
    }

    // ── Mixed wildcards ──

    [Fact]
    public void Plus_and_hash_combined()
    {
        _router.Subscribe("c1", "+/sensors/#");
        Assert.Single(_router.Match("home/sensors/temp"));
        Assert.Single(_router.Match("office/sensors/a/b"));
        Assert.Empty(_router.Match("sensors/temp"));
    }

    [Fact]
    public void Exact_and_wildcard_both_match()
    {
        _router.Subscribe("c1", "sensors/temp");
        _router.Subscribe("c2", "sensors/+");
        _router.Subscribe("c3", "sensors/#");

        var result = _router.Match("sensors/temp");
        Assert.Equal(3, result.Count);
        Assert.Contains("c1", result);
        Assert.Contains("c2", result);
        Assert.Contains("c3", result);
    }

    // ── Unsubscribe ──

    [Fact]
    public void Unsubscribe_removes_subscription()
    {
        _router.Subscribe("c1", "a/b");
        _router.Unsubscribe("c1", "a/b");
        Assert.Empty(_router.Match("a/b"));
    }

    [Fact]
    public void Unsubscribe_nonexistent_is_noop()
    {
        _router.Unsubscribe("c1", "no/such/topic"); // no throw
    }

    [Fact]
    public void Unsubscribe_only_affects_target()
    {
        _router.Subscribe("c1", "a/b");
        _router.Subscribe("c2", "a/b");
        _router.Unsubscribe("c1", "a/b");

        var result = _router.Match("a/b");
        Assert.Single(result);
        Assert.Equal("c2", result[0]);
    }

    // ── UnsubscribeAll ──

    [Fact]
    public void UnsubscribeAll_removes_from_all_topics()
    {
        _router.Subscribe("c1", "a/b");
        _router.Subscribe("c1", "x/y");
        _router.Subscribe("c1", "sensors/#");
        _router.Subscribe("c2", "a/b");

        _router.UnsubscribeAll("c1");

        Assert.Empty(_router.Match("x/y"));
        Assert.Empty(_router.Match("sensors/temp"));
        // c2 still subscribed
        Assert.Single(_router.Match("a/b"));
    }

    // ── Matches static method ──

    [Theory]
    [InlineData("a/b", "a/b", true)]
    [InlineData("a/b", "a/c", false)]
    [InlineData("a/b", "+/b", true)]
    [InlineData("a/b", "a/+", true)]
    [InlineData("a/b", "+/+", true)]
    [InlineData("a/b/c", "a/+/c", true)]
    [InlineData("a/b/c", "a/#", true)]
    [InlineData("a/b/c", "#", true)]
    [InlineData("a/b", "a/b/c", false)]
    [InlineData("a/b/c", "a/b", false)]
    [InlineData("a", "a", true)]
    [InlineData("a", "b", false)]
    [InlineData("a", "+", true)]
    [InlineData("a", "#", true)]
    [InlineData("a/b", "a/#", true)]
    public void Matches_static(string topic, string pattern, bool expected)
    {
        Assert.Equal(expected, TopicRouter.Matches(topic, pattern));
    }

    // ── Duplicate subscribe ──

    [Fact]
    public void Duplicate_subscribe_does_not_duplicate_match()
    {
        _router.Subscribe("c1", "a/b");
        _router.Subscribe("c1", "a/b");
        Assert.Single(_router.Match("a/b"));
    }

    // ── Concurrency ──

    [Fact]
    public async Task Concurrent_subscribe_and_match()
    {
        var subscribeTasks = Enumerable.Range(0, 50).Select(i =>
            Task.Run(() => _router.Subscribe($"c{i}", "topic/test"))
        ).ToArray();

        var matchTasks = Enumerable.Range(0, 50).Select(_ =>
            Task.Run(() => _router.Match("topic/test"))
        ).ToArray();

        await Task.WhenAll(subscribeTasks.Concat(matchTasks));

        var final = _router.Match("topic/test");
        Assert.Equal(50, final.Count);
    }
}
