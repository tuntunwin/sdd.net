#if ENABLE_RAFT
using System.Text.Json;
using StateBroker.Core;

namespace StateBroker.Raft;

/// <summary>
/// Fans out state changes to local subscribers after Raft Apply.
/// All methods are sync (Channel.TryWrite is non-blocking).
/// </summary>
public sealed class SubscriberNotifier : ISubscriberNotifier
{
    private readonly TopicRouter _router;
    private readonly SessionManager _sessions;

    public SubscriberNotifier(TopicRouter router, SessionManager sessions)
    {
        _router = router;
        _sessions = sessions;
    }

    public void NotifySet(string topic, JsonElement payload)
    {
        var targets = _router.Match(topic);
        foreach (var clientId in targets)
        {
            var deliver = new Frame(
                Frame.Types.Deliver,
                topic,
                Qos: 1,
                MsgId: Guid.NewGuid().ToString("N"),
                Payload: payload);

            _sessions.TrySend(clientId, deliver);
        }
    }

    public void NotifyDelete(string topic)
    {
        var targets = _router.Match(topic);
        foreach (var clientId in targets)
        {
            var deliver = new Frame(
                Frame.Types.Deliver,
                topic,
                Qos: 1,
                MsgId: Guid.NewGuid().ToString("N"));

            _sessions.TrySend(clientId, deliver);
        }
    }
}
#endif
