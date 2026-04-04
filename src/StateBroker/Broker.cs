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

        // Replay pending QoS 1 frames on reconnect
        foreach (var pending in _qos.GetPending(conn.ClientId))
            session.TrySend(pending);

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
                writeTask.ContinueWith(_ => { }, TaskScheduler.Default));
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
        if (frame.Retain)
            await _store.SetAsync(frame.Topic!, frame.Payload, ct);

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

        // Push retained state matching the subscription pattern
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
