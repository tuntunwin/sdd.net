#if ENABLE_RAFT
using System.Text.Json;

namespace StateBroker.Raft;

/// <summary>
/// Sync-only notification interface called from BrokerStateMachine.ApplyAsync.
/// Must not await — runs under DotNext internal lock.
/// </summary>
public interface ISubscriberNotifier
{
    void NotifySet(string topic, JsonElement payload);
    void NotifyDelete(string topic);
}
#endif
