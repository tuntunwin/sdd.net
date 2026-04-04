using System.Text.Json;

namespace StateBroker.Core;

public enum StateOp { Set, Delete }

public sealed record StateEntry(
    string      Topic,
    JsonElement Payload,
    StateOp     Op
);
