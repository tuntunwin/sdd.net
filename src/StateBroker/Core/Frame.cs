using System.Text.Json;
using System.Text.Json.Serialization;

namespace StateBroker.Core;

/// <summary>
/// Wire frame — NDJSON over WebSocket.
/// </summary>
public sealed record Frame(
    string      Type,
    string?     Topic    = null,
    int         Qos      = 0,
    string?     MsgId    = null,
    bool        Retain   = false,
    bool        Retained = false,
    bool        Dup      = false,
    [property: JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
    JsonElement Payload  = default
)
{
    public static class Types
    {
        public const string Publish   = "PUBLISH";
        public const string Subscribe = "SUBSCRIBE";
        public const string SubAck    = "SUBACK";
        public const string Ack       = "ACK";
        public const string Deliver   = "DELIVER";
        public const string Ping      = "PING";
        public const string Pong      = "PONG";
        public const string Redirect  = "REDIRECT";
    }
}
