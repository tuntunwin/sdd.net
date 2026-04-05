using System.Text.Json;
using StateBroker.Core;

namespace StateBroker;

public sealed class Broker
{
    private readonly ITransport _transport;
    private readonly IStateStore _store;
    private readonly IConsensusLayer _consensus;
    private readonly SessionManager _sessions;
    private readonly TopicRouter _router;
    private readonly IQosEngine _qos;

    public Broker(
        ITransport transport,
        IStateStore store,
        IConsensusLayer consensus,
        SessionManager sessions,
        TopicRouter router,
        IQosEngine qos)
    {
        _transport = transport;
        _store = store;
        _consensus = consensus;
        _sessions = sessions;
        _router = router;
        _qos = qos;
    }

    public SessionManager Sessions => _sessions;
    public IStateStore Store => _store;
    public TopicRouter Router => _router;
    public IQosEngine Qos => _qos;

    public async Task RunAsync(CancellationToken ct)
    {
        await foreach (var conn in _transport.AcceptAsync(ct))
        {
            _ = HandleConnectionAsync(conn, ct);
        }
    }

    private async Task HandleConnectionAsync(IConnection conn, CancellationToken ct)
    {
        var session = _sessions.GetOrCreate(conn.ClientId);
        using var connCts = CancellationTokenSource.CreateLinkedTokenSource(ct, session.CancellationToken);
        var lct = connCts.Token;

        var retryTask = _qos.StartRetryLoopAsync(conn.ClientId, _sessions, lct);

        var writeTask = WriteLoopAsync(conn, session, lct);
        var readTask = ReadLoopAsync(conn, session, lct);

        try
        {
            await Task.WhenAny(readTask, writeTask);
        }
        finally
        {
            await connCts.CancelAsync();
            await conn.CloseAsync();
            await Task.WhenAll(
                readTask.ContinueWith(_ => { }, TaskScheduler.Default),
                writeTask.ContinueWith(_ => { }, TaskScheduler.Default),
                retryTask.ContinueWith(_ => { }, TaskScheduler.Default));
        }
    }

    private static async Task WriteLoopAsync(IConnection conn, Session session, CancellationToken ct)
    {
        await foreach (var frame in session.SendChannel.Reader.ReadAllAsync(ct))
        {
            await conn.SendAsync(frame, ct);
        }
    }

    private async Task ReadLoopAsync(IConnection conn, Session session, CancellationToken ct)
    {
        await foreach (var frame in conn.ReadFramesAsync(ct))
        {
            await DispatchAsync(frame, session, ct);
        }
    }

    private async ValueTask DispatchAsync(Frame frame, Session session, CancellationToken ct)
    {
        switch (frame.Type)
        {
            case Frame.Types.Ping:
                session.TrySend(new Frame(Frame.Types.Pong));
                break;

            case Frame.Types.Publish when frame.Topic is not null:
                await HandlePublishAsync(frame, session, ct);
                break;

            case Frame.Types.Subscribe when frame.Topic is not null:
                await HandleSubscribeAsync(frame, session, ct);
                break;

            case Frame.Types.Ack when frame.MsgId is not null:
                await _qos.AckAsync(session.ClientId, frame.MsgId, ct);
                break;
        }
    }

    private async ValueTask HandlePublishAsync(Frame frame, Session session, CancellationToken ct)
    {
        var isRetainedDelete = frame.Retain
            && (frame.Payload.ValueKind == JsonValueKind.Undefined
                || (frame.Payload.ValueKind == JsonValueKind.String
                    && frame.Payload.GetString()?.Length == 0));

        // ── Retained state mutation ──
        if (frame.Retain)
        {
            if (_consensus.RequiresProposal)
            {
                // Raft mode: retained writes must go through consensus
                if (!_consensus.IsLeader)
                {
                    // Redirect to leader — no PUBACK, client reconnects
                    var redirect = new LeaderRedirect(_consensus.LeaderEndpoint);
                    session.TrySend(new Frame(Frame.Types.Redirect,
                        Payload: JsonSerializer.SerializeToElement(
                            redirect, BrokerJsonContext.Default.LeaderRedirect)));
                    return;
                }

                // Leader: propose through Raft → ApplyAsync calls SetMemory on all nodes
                var op = isRetainedDelete ? StateOp.Delete : StateOp.Set;
                var entry = new StateEntry(frame.Topic!, frame.Payload, op);
                var data = JsonSerializer.SerializeToUtf8Bytes(
                    entry, BrokerJsonContext.Default.StateEntry);
                await _consensus.ProposeAsync(data, ct);
                // After ProposeAsync returns, quorum committed and ApplyAsync ran
            }
            else
            {
                // Single-node mode: write directly via WAL
                if (isRetainedDelete)
                    await _store.DeleteAsync(frame.Topic!, ct);
                else
                    await _store.SetAsync(frame.Topic!, frame.Payload, ct);
            }
        }

        // PUBACK — confirm receipt of QoS 1 publish to the publisher
        if (frame.Qos >= 1 && frame.MsgId is not null)
            session.TrySend(new Frame(Frame.Types.Ack, MsgId: frame.MsgId));

        // Fan-out to matching subscribers (local to this node)
        var targets = _router.Match(frame.Topic!);
        foreach (var clientId in targets)
        {
            var deliver = new Frame(
                Frame.Types.Deliver,
                frame.Topic,
                frame.Qos,
                MsgId: frame.Qos >= 1 ? Guid.NewGuid().ToString("N") : null,
                Payload: frame.Payload);

            if (frame.Qos >= 1)
                await _qos.EnqueueAsync(clientId, deliver, ct);

            _sessions.TrySend(clientId, deliver);
        }
    }

    private async ValueTask HandleSubscribeAsync(Frame frame, Session session, CancellationToken ct)
    {
        _router.Subscribe(session.ClientId, frame.Topic!);

        // SUBACK — confirm subscription accepted
        session.TrySend(new Frame(Frame.Types.SubAck, Topic: frame.Topic));

        // Push retained state matching the subscription pattern via QoS 1.
        // EnqueueAsync replaces any stale outbox entry for the same topic.
        foreach (var (topic, payload) in _store.GetAll())
        {
            if (!TopicRouter.Matches(topic, frame.Topic!))
                continue;

            var deliver = new Frame(
                Frame.Types.Deliver,
                topic,
                Qos: 1,
                MsgId: Guid.NewGuid().ToString("N"),
                Retained: true,
                Payload: payload);

            await _qos.EnqueueAsync(session.ClientId, deliver, ct);
            session.TrySend(deliver);
        }
    }
}
