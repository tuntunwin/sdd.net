using StateBroker.Core;

namespace StateBroker;

public sealed class Broker
{
    private readonly ITransport _transport;
    private readonly IStateStore _store;
    private readonly IConsensusLayer _consensus;
    private readonly SessionManager _sessions;

    public Broker(
        ITransport transport,
        IStateStore store,
        IConsensusLayer consensus,
        SessionManager sessions)
    {
        _transport = transport;
        _store = store;
        _consensus = consensus;
        _sessions = sessions;
    }

    public SessionManager Sessions => _sessions;
    public IStateStore Store => _store;

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

        var writeTask = WriteLoopAsync(conn, session, lct);
        var readTask = ReadLoopAsync(conn, session, lct);

        try
        {
            await Task.WhenAny(readTask, writeTask);
        }
        finally
        {
            // Cancel the remaining loop when one ends
            await connCts.CancelAsync();
            await conn.CloseAsync();
            // Swallow exceptions from the other task
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
                if (frame.Retain)
                    await _store.SetAsync(frame.Topic, frame.Payload, ct);
                // Fan-out to subscribers will be added with message router (step 6)
                break;
        }
    }
}
