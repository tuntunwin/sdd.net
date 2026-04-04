using System.Text.Json;
using StateBroker.Core;

namespace StateBroker.Tests;

public class QosEngineTests
{
    private readonly QosEngine _qos = new();

    private static Frame MakeDeliver(string msgId, string topic = "t") =>
        new(Frame.Types.Deliver, topic, Qos: 1, MsgId: msgId,
            Payload: JsonDocument.Parse("1").RootElement);

    // ── Enqueue + GetPending ──

    [Fact]
    public async Task Enqueue_and_GetPending()
    {
        var f1 = MakeDeliver("m1");
        var f2 = MakeDeliver("m2");

        await _qos.EnqueueAsync("c1", f1, CancellationToken.None);
        await _qos.EnqueueAsync("c1", f2, CancellationToken.None);

        var pending = _qos.GetPending("c1");
        Assert.Equal(2, pending.Count);
        Assert.Equal("m1", pending[0].MsgId);
        Assert.Equal("m2", pending[1].MsgId);
    }

    [Fact]
    public void GetPending_unknown_client_returns_empty()
    {
        Assert.Empty(_qos.GetPending("unknown"));
    }

    [Fact]
    public async Task GetPending_returns_copy()
    {
        await _qos.EnqueueAsync("c1", MakeDeliver("m1"), CancellationToken.None);

        var p1 = _qos.GetPending("c1");
        var p2 = _qos.GetPending("c1");

        Assert.NotSame(p1, p2);
        Assert.Equal(p1.Count, p2.Count);
    }

    // ── Ack ──

    [Fact]
    public async Task Ack_removes_specific_message()
    {
        await _qos.EnqueueAsync("c1", MakeDeliver("m1"), CancellationToken.None);
        await _qos.EnqueueAsync("c1", MakeDeliver("m2"), CancellationToken.None);
        await _qos.EnqueueAsync("c1", MakeDeliver("m3"), CancellationToken.None);

        await _qos.AckAsync("c1", "m2", CancellationToken.None);

        var pending = _qos.GetPending("c1");
        Assert.Equal(2, pending.Count);
        Assert.Equal("m1", pending[0].MsgId);
        Assert.Equal("m3", pending[1].MsgId);
    }

    [Fact]
    public async Task Ack_unknown_client_is_noop()
    {
        await _qos.AckAsync("unknown", "m1", CancellationToken.None); // no throw
    }

    [Fact]
    public async Task Ack_unknown_msgId_is_noop()
    {
        await _qos.EnqueueAsync("c1", MakeDeliver("m1"), CancellationToken.None);
        await _qos.AckAsync("c1", "nonexistent", CancellationToken.None);

        Assert.Single(_qos.GetPending("c1"));
    }

    [Fact]
    public async Task Ack_all_messages_leaves_empty()
    {
        await _qos.EnqueueAsync("c1", MakeDeliver("m1"), CancellationToken.None);
        await _qos.EnqueueAsync("c1", MakeDeliver("m2"), CancellationToken.None);

        await _qos.AckAsync("c1", "m1", CancellationToken.None);
        await _qos.AckAsync("c1", "m2", CancellationToken.None);

        Assert.Empty(_qos.GetPending("c1"));
    }

    // ── Evict ──

    [Fact]
    public async Task Evict_clears_outbox()
    {
        await _qos.EnqueueAsync("c1", MakeDeliver("m1"), CancellationToken.None);
        await _qos.EnqueueAsync("c1", MakeDeliver("m2"), CancellationToken.None);

        await _qos.EvictAsync("c1", CancellationToken.None);

        Assert.Empty(_qos.GetPending("c1"));
    }

    [Fact]
    public async Task Evict_unknown_client_is_noop()
    {
        await _qos.EvictAsync("unknown", CancellationToken.None); // no throw
    }

    // ── Multiple clients ──

    [Fact]
    public async Task Separate_outboxes_per_client()
    {
        await _qos.EnqueueAsync("c1", MakeDeliver("m1"), CancellationToken.None);
        await _qos.EnqueueAsync("c2", MakeDeliver("m2"), CancellationToken.None);

        Assert.Single(_qos.GetPending("c1"));
        Assert.Single(_qos.GetPending("c2"));

        await _qos.AckAsync("c1", "m1", CancellationToken.None);

        Assert.Empty(_qos.GetPending("c1"));
        Assert.Single(_qos.GetPending("c2"));
    }

    // ── Concurrency ──

    [Fact]
    public async Task Concurrent_enqueue_and_ack()
    {
        // Enqueue 100 messages
        var enqueueTasks = Enumerable.Range(0, 100).Select(i =>
            _qos.EnqueueAsync("c1", MakeDeliver($"m{i}"), CancellationToken.None).AsTask()
        ).ToArray();
        await Task.WhenAll(enqueueTasks);

        // Ack first 50 concurrently
        var ackTasks = Enumerable.Range(0, 50).Select(i =>
            _qos.AckAsync("c1", $"m{i}", CancellationToken.None).AsTask()
        ).ToArray();
        await Task.WhenAll(ackTasks);

        var pending = _qos.GetPending("c1");
        Assert.Equal(50, pending.Count);
    }

    [Fact]
    public async Task Concurrent_enqueue_across_clients()
    {
        var tasks = Enumerable.Range(0, 100).Select(i =>
            _qos.EnqueueAsync($"c{i % 10}", MakeDeliver($"m{i}"), CancellationToken.None).AsTask()
        ).ToArray();
        await Task.WhenAll(tasks);

        for (var c = 0; c < 10; c++)
            Assert.Equal(10, _qos.GetPending($"c{c}").Count);
    }
}
