using System.Collections.Concurrent;

namespace StateBroker.Core;

public sealed class SessionManager
{
    private readonly ConcurrentDictionary<string, Session> _sessions = new();
    private readonly int _sendBufferDepth;

    public SessionManager(int sendBufferDepth = 256)
    {
        _sendBufferDepth = sendBufferDepth;
    }

    public Session GetOrCreate(string clientId) =>
        _sessions.GetOrAdd(clientId, id => new Session(id, _sendBufferDepth));

    public bool TryGet(string clientId, out Session? session) =>
        _sessions.TryGetValue(clientId, out session);

    public bool TrySend(string clientId, Frame frame)
    {
        if (_sessions.TryGetValue(clientId, out var session))
            return session.TrySend(frame);
        return false;
    }

    public Session? Remove(string clientId)
    {
        _sessions.TryRemove(clientId, out var session);
        return session;
    }

    public int Count => _sessions.Count;

    public IReadOnlyList<string> ConnectedClientIds => [.. _sessions.Keys];
}
