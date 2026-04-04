using System.Text.Json;

namespace StateBroker.Core;

public sealed record WalEntry(
    long        Seq,
    string      Topic,
    JsonElement Payload
);
