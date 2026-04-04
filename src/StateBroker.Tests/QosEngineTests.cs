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
        await _qos.EnqueueAsync("c1", MakeDeliver("m1", "t1"), CancellationToken.None);
        await _qos.EnqueueAsync("c1", MakeDeliver("m2", "t2"), CancellationToken.None);

        var pending = _qos.GetPending("c1");
        Assert.Equal(2, pending.Count);
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

    // ── Topic deduplication ──

    [Fact]
    public async Task Enqueue_replaces_same_topic()
    {
        var f1 = MakeDeliver("m1", "sensors/temp");
        var f2 = new Frame(Frame.Types.Deliver, "sensors/temp", Qos: 1, MsgId: "m2",
            Payload: JsonDocument.Parse("999").RootElement);

        await _qos.EnqueueAsync("c1", f1, CancellationToken.None);
        await _qos.EnqueueAsync("c1", f2, CancellationToken.None);

        var pending = _qos.GetPending("c1");
        Assert.Single(pending);
        Assert.Equal("m2", pending[0].MsgId);
        Assert.Equal(999, pending[0].Payload.GetInt32());
    }

    [Fact]
    public async Task Enqueue_different_topics_accumulates()
    {
        await _qos.EnqueueAsync("c1", MakeDeliver("m1", "a"), CancellationToken.None);
        await _qos.EnqueueAsync("c1", MakeDeliver("m2", "b"), CancellationToken.None);

        Assert.Equal(2, _qos.GetPending("c1").Count);
    }

    [Fact]
    public async Task Ack_stale_msgId_is_noop()
    {
        // Enqueue m1 for topic "t", then m2 replaces it
        await _qos.EnqueueAsync("c1", MakeDeliver("m1", "t"), CancellationToken.None);
        await _qos.EnqueueAsync("c1", MakeDeliver("m2", "t"), CancellationToken.None);

        // ACK stale m1 — already replaced, should be no-op
        await _qos.AckAsync("c1", "m1", CancellationToken.None);
        var pending = _qos.GetPending("c1");
        Assert.Single(pending);
        Assert.Equal("m2", pending[0].MsgId);

        // ACK current m2 — removes it
        await _qos.AckAsync("c1", "m2", CancellationToken.None);
        Assert.Empty(_qos.GetPending("c1"));
    }

    // ── Ack ──

    [Fact]
    public async Task Ack_removes_specific_message()
    {
        await _qos.EnqueueAsync("c1", MakeDeliver("m1", "t1"), CancellationToken.None);
        await _qos.EnqueueAsync("c1", MakeDeliver("m2", "t2"), CancellationToken.None);
        await _qos.EnqueueAsync("c1", MakeDeliver("m3", "t3"), CancellationToken.None);

        await _qos.AckAsync("c1", "m2", CancellationToken.None);

        var pending = _qos.GetPending("c1");
        Assert.Equal(2, pending.Count);
        Assert.DoesNotContain(pending, f => f.MsgId == "m2");
    }

    [Fact]
    public async Task Ack_unknown_client_is_noop()
    {
        await _qos.AckAsync("unknown", "m1", CancellationToken.None);
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
        await _qos.EnqueueAsync("c1", MakeDeliver("m1", "t1"), CancellationToken.None);
        await _qos.EnqueueAsync("c1", MakeDeliver("m2", "t2"), CancellationToken.None);

        await _qos.AckAsync("c1", "m1", CancellationToken.None);
        await _qos.AckAsync("c1", "m2", CancellationToken.None);

        Assert.Empty(_qos.GetPending("c1"));
    }

    // ── Evict ──

    [Fact]
    public async Task Evict_clears_outbox()
    {
        await _qos.EnqueueAsync("c1", MakeDeliver("m1", "t1"), CancellationToken.None);
        await _qos.EnqueueAsync("c1", MakeDeliver("m2", "t2"), CancellationToken.None);

        await _qos.EvictAsync("c1", CancellationToken.None);

        Assert.Empty(_qos.GetPending("c1"));
    }

    [Fact]
    public async Task Evict_unknown_client_is_noop()
    {
        await _qos.EvictAsync("unknown", CancellationToken.None);
    }

    // ── EvictTopic ──

    [Fact]
    public async Task EvictTopic_removes_specific_topic()
    {
        await _qos.EnqueueAsync("c1", MakeDeliver("m1", "t1"), CancellationToken.None);
        await _qos.EnqueueAsync("c1", MakeDeliver("m2", "t2"), CancellationToken.None);
        await _qos.EnqueueAsync("c1", MakeDeliver("m3", "t3"), CancellationToken.None);

        await _qos.EvictTopicAsync("c1", "t2", CancellationToken.None);

        var pending = _qos.GetPending("c1");
        Assert.Equal(2, pending.Count);
        Assert.DoesNotContain(pending, f => f.Topic == "t2");
        Assert.Contains(pending, f => f.Topic == "t1");
        Assert.Contains(pending, f => f.Topic == "t3");
    }

    [Fact]
    public async Task EvictTopic_unknown_client_is_noop()
    {
        await _qos.EvictTopicAsync("unknown", "t1", CancellationToken.None);
    }

    [Fact]
    public async Task EvictTopic_unknown_topic_is_noop()
    {
        await _qos.EnqueueAsync("c1", MakeDeliver("m1", "t1"), CancellationToken.None);
        await _qos.EvictTopicAsync("c1", "nonexistent", CancellationToken.None);

        Assert.Single(_qos.GetPending("c1"));
    }

    // ── Multiple clients ──

    [Fact]
    public async Task Separate_outboxes_per_client()
    {
        await _qos.EnqueueAsync("c1", MakeDeliver("m1", "t1"), CancellationToken.None);
        await _qos.EnqueueAsync("c2", MakeDeliver("m2", "t1"), CancellationToken.None);

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
        // Enqueue 100 messages on distinct topics
        var enqueueTasks = Enumerable.Range(0, 100).Select(i =>
            _qos.EnqueueAsync("c1", MakeDeliver($"m{i}", $"t/{i}"), CancellationToken.None).AsTask()
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
        // 10 clients, 10 distinct topics each
        var tasks = Enumerable.Range(0, 100).Select(i =>
            _qos.EnqueueAsync($"c{i % 10}", MakeDeliver($"m{i}", $"t/{i}"), CancellationToken.None).AsTask()
        ).ToArray();
        await Task.WhenAll(tasks);

        for (var c = 0; c < 10; c++)
            Assert.Equal(10, _qos.GetPending($"c{c}").Count);
    }

    // ── Retry loop ──

    [Fact]
    public async Task Retry_loop_resends_pending_frames()
    {
        var qos = new QosEngine(
            retryBase: TimeSpan.FromMilliseconds(50),
            retryMax: TimeSpan.FromMilliseconds(200));
        var sessions = new SessionManager();
        var session = sessions.GetOrCreate("c1");

        await qos.EnqueueAsync("c1", MakeDeliver("m1"), CancellationToken.None);

        using var cts = new CancellationTokenSource();
        var retryTask = qos.StartRetryLoopAsync("c1", sessions, cts.Token);

        await Task.Delay(150);

        Assert.True(session.SendChannel.Reader.TryRead(out var frame));
        Assert.Equal("m1", frame!.MsgId);

        cts.Cancel();
        await retryTask;
    }

    [Fact]
    public async Task Retry_loop_stops_after_ack()
    {
        var qos = new QosEngine(
            retryBase: TimeSpan.FromMilliseconds(50),
            retryMax: TimeSpan.FromMilliseconds(200));
        var sessions = new SessionManager();
        sessions.GetOrCreate("c1");

        await qos.EnqueueAsync("c1", MakeDeliver("m1"), CancellationToken.None);

        using var cts = new CancellationTokenSource();
        var retryTask = qos.StartRetryLoopAsync("c1", sessions, cts.Token);

        await qos.AckAsync("c1", "m1", CancellationToken.None);
        await Task.Delay(200);

        Assert.Empty(qos.GetPending("c1"));

        cts.Cancel();
        await retryTask;
    }

    [Fact]
    public async Task Retry_loop_cancels_on_disconnect()
    {
        var qos = new QosEngine(
            retryBase: TimeSpan.FromMilliseconds(50),
            retryMax: TimeSpan.FromMilliseconds(200));
        var sessions = new SessionManager();
        sessions.GetOrCreate("c1");

        await qos.EnqueueAsync("c1", MakeDeliver("m1"), CancellationToken.None);

        using var cts = new CancellationTokenSource();
        var retryTask = qos.StartRetryLoopAsync("c1", sessions, cts.Token);

        cts.Cancel();
        await retryTask;
    }

    [Fact]
    public async Task Retry_only_sends_latest_value_per_topic()
    {
        var qos = new QosEngine(
            retryBase: TimeSpan.FromMilliseconds(50),
            retryMax: TimeSpan.FromMilliseconds(200));
        var sessions = new SessionManager();
        var session = sessions.GetOrCreate("c1");

        // Publish 222 then 333 to same topic — only 333 should be retried
        var f1 = new Frame(Frame.Types.Deliver, "sensors/temp", Qos: 1, MsgId: "m1",
            Payload: JsonDocument.Parse("222").RootElement);
        var f2 = new Frame(Frame.Types.Deliver, "sensors/temp", Qos: 1, MsgId: "m2",
            Payload: JsonDocument.Parse("333").RootElement);

        await qos.EnqueueAsync("c1", f1, CancellationToken.None);
        await qos.EnqueueAsync("c1", f2, CancellationToken.None);

        Assert.Single(qos.GetPending("c1"));

        using var cts = new CancellationTokenSource();
        var retryTask = qos.StartRetryLoopAsync("c1", sessions, cts.Token);

        await Task.Delay(150);

        // Drain all retried frames — all should be the latest value only
        var frames = new List<Frame>();
        while (session.SendChannel.Reader.TryRead(out var f))
            frames.Add(f);

        Assert.True(frames.Count >= 1);
        Assert.All(frames, f =>
        {
            Assert.Equal("m2", f.MsgId);
            Assert.Equal(333, f.Payload.GetInt32());
        });

        cts.Cancel();
        await retryTask;
    }

    [Fact]
    public async Task Skip_retries_then_ack_clears_outbox()
    {
        var qos = new QosEngine(
            retryBase: TimeSpan.FromMilliseconds(50),
            retryMax: TimeSpan.FromMilliseconds(200));
        var sessions = new SessionManager();
        var session = sessions.GetOrCreate("c1");

        await qos.EnqueueAsync("c1", MakeDeliver("m1"), CancellationToken.None);

        using var cts = new CancellationTokenSource();
        var retryTask = qos.StartRetryLoopAsync("c1", sessions, cts.Token);

        // Drain 3 retries without ACKing
        for (var i = 0; i < 3; i++)
        {
            await Task.Delay(100);
            while (session.SendChannel.Reader.TryRead(out _)) { }
        }

        // Outbox still has the frame
        Assert.Single(qos.GetPending("c1"));

        // ACK after skipping
        await qos.AckAsync("c1", "m1", CancellationToken.None);
        Assert.Empty(qos.GetPending("c1"));

        // Wait to confirm no more retries
        while (session.SendChannel.Reader.TryRead(out _)) { }
        await Task.Delay(150);
        Assert.False(session.SendChannel.Reader.TryRead(out _));

        cts.Cancel();
        await retryTask;
    }

    [Fact]
    public async Task New_value_replaces_during_retry_loop()
    {
        var qos = new QosEngine(
            retryBase: TimeSpan.FromMilliseconds(50),
            retryMax: TimeSpan.FromMilliseconds(200));
        var sessions = new SessionManager();
        var session = sessions.GetOrCreate("c1");

        // Enqueue v1
        var v1 = new Frame(Frame.Types.Deliver, "s/t", Qos: 1, MsgId: "m1",
            Payload: JsonDocument.Parse("100").RootElement);
        await qos.EnqueueAsync("c1", v1, CancellationToken.None);

        using var cts = new CancellationTokenSource();
        var retryTask = qos.StartRetryLoopAsync("c1", sessions, cts.Token);

        // Let a retry of v1 happen
        await Task.Delay(100);
        while (session.SendChannel.Reader.TryRead(out var f))
            Assert.Equal("m1", f.MsgId);

        // Enqueue v2 on same topic — replaces v1
        var v2 = new Frame(Frame.Types.Deliver, "s/t", Qos: 1, MsgId: "m2",
            Payload: JsonDocument.Parse("200").RootElement);
        await qos.EnqueueAsync("c1", v2, CancellationToken.None);

        // Next retries should be v2 only
        await Task.Delay(150);
        var frames = new List<Frame>();
        while (session.SendChannel.Reader.TryRead(out var fr))
            frames.Add(fr);

        Assert.True(frames.Count >= 1);
        Assert.All(frames, fr =>
        {
            Assert.Equal("m2", fr.MsgId);
            Assert.Equal(200, fr.Payload.GetInt32());
        });

        // ACK v2 — done
        await qos.AckAsync("c1", "m2", CancellationToken.None);
        Assert.Empty(qos.GetPending("c1"));

        cts.Cancel();
        await retryTask;
    }

    [Fact]
    public async Task Ack_stale_msgId_during_retry_is_noop()
    {
        var qos = new QosEngine(
            retryBase: TimeSpan.FromMilliseconds(50),
            retryMax: TimeSpan.FromMilliseconds(200));
        var sessions = new SessionManager();
        sessions.GetOrCreate("c1");

        // v1 then v2 on same topic
        await qos.EnqueueAsync("c1",
            new Frame(Frame.Types.Deliver, "t", Qos: 1, MsgId: "old",
                Payload: JsonDocument.Parse("1").RootElement),
            CancellationToken.None);
        await qos.EnqueueAsync("c1",
            new Frame(Frame.Types.Deliver, "t", Qos: 1, MsgId: "new",
                Payload: JsonDocument.Parse("2").RootElement),
            CancellationToken.None);

        // ACK stale "old" — no-op
        await qos.AckAsync("c1", "old", CancellationToken.None);
        var pending = qos.GetPending("c1");
        Assert.Single(pending);
        Assert.Equal("new", pending[0].MsgId);

        // ACK "new" — clears
        await qos.AckAsync("c1", "new", CancellationToken.None);
        Assert.Empty(qos.GetPending("c1"));
    }
}
