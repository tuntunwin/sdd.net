using System.Text.Json;

namespace StateBroker.Core;

public sealed record WalEntry(
    long        Seq,
    string      Topic,
    [property: System.Text.Json.Serialization.JsonIgnore(
        Condition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingDefault)]
    JsonElement Payload = default,
    bool        Delete  = false
);
