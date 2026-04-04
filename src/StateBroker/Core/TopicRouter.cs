namespace StateBroker.Core;

public sealed class TopicRouter : IDisposable
{
    private readonly ReaderWriterLockSlim _lock = new(LockRecursionPolicy.NoRecursion);
    private readonly TrieNode _root = new();

    public void Subscribe(string clientId, string pattern)
    {
        var segments = pattern.Split('/');
        _lock.EnterWriteLock();
        try
        {
            var node = _root;
            foreach (var seg in segments)
                node = node.GetOrAddChild(seg);
            node.Subscribers.Add(clientId);
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    public void Unsubscribe(string clientId, string pattern)
    {
        var segments = pattern.Split('/');
        _lock.EnterWriteLock();
        try
        {
            var node = _root;
            foreach (var seg in segments)
            {
                if (!node.Children.TryGetValue(seg, out var child))
                    return;
                node = child;
            }
            node.Subscribers.Remove(clientId);
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    public void UnsubscribeAll(string clientId)
    {
        _lock.EnterWriteLock();
        try
        {
            RemoveFromAll(_root, clientId);
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    public IReadOnlyList<string> Match(string topic)
    {
        var segments = topic.Split('/');
        var result = new HashSet<string>();
        _lock.EnterReadLock();
        try
        {
            MatchRecursive(_root, segments, 0, result);
        }
        finally
        {
            _lock.ExitReadLock();
        }
        return [.. result];
    }

    /// <summary>
    /// Checks if a concrete topic matches a subscription pattern.
    /// Used for retained-state push on subscribe.
    /// </summary>
    public static bool Matches(string topic, string pattern)
    {
        var t = topic.Split('/');
        var p = pattern.Split('/');
        return MatchSegments(t, 0, p, 0);
    }

    private static bool MatchSegments(string[] topic, int ti, string[] pattern, int pi)
    {
        if (pi == pattern.Length)
            return ti == topic.Length;
        if (pattern[pi] == "#")
            return true;
        if (ti == topic.Length)
            return false;
        if (pattern[pi] == "+" || pattern[pi] == topic[ti])
            return MatchSegments(topic, ti + 1, pattern, pi + 1);
        return false;
    }

    private static void MatchRecursive(
        TrieNode node, string[] segments, int depth, HashSet<string> result)
    {
        // # at current node matches everything from here
        if (node.Children.TryGetValue("#", out var hashNode))
        {
            foreach (var sub in hashNode.Subscribers)
                result.Add(sub);
        }

        if (depth == segments.Length)
        {
            // Exact match — collect subscribers at this node
            foreach (var sub in node.Subscribers)
                result.Add(sub);
            return;
        }

        var seg = segments[depth];

        // Exact segment match
        if (node.Children.TryGetValue(seg, out var exact))
            MatchRecursive(exact, segments, depth + 1, result);

        // + wildcard matches any single segment
        if (node.Children.TryGetValue("+", out var plus))
            MatchRecursive(plus, segments, depth + 1, result);
    }

    private static void RemoveFromAll(TrieNode node, string clientId)
    {
        node.Subscribers.Remove(clientId);
        foreach (var child in node.Children.Values)
            RemoveFromAll(child, clientId);
    }

    public void Dispose() => _lock.Dispose();

    private sealed class TrieNode
    {
        public Dictionary<string, TrieNode> Children { get; } = new();
        public HashSet<string> Subscribers { get; } = new();

        public TrieNode GetOrAddChild(string segment)
        {
            if (!Children.TryGetValue(segment, out var child))
            {
                child = new TrieNode();
                Children[segment] = child;
            }
            return child;
        }
    }
}
