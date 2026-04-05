using System.Text.Json;
using System.Text.Json.Serialization;

namespace StateBroker.Core;

[JsonSerializable(typeof(Frame))]
[JsonSerializable(typeof(WalEntry))]
[JsonSerializable(typeof(StateEntry))]
[JsonSerializable(typeof(StateEntry[]))]
[JsonSerializable(typeof(BrokerConfig))]
[JsonSerializable(typeof(StoreConfig))]
[JsonSerializable(typeof(QosConfig))]
[JsonSerializable(typeof(SessionConfig))]
[JsonSerializable(typeof(ClusterConfig))]
[JsonSerializable(typeof(JsonElement))]
[JsonSerializable(typeof(LeaderRedirect))]
[JsonSourceGenerationOptions(
    PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase,
    DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull)]
internal partial class BrokerJsonContext : JsonSerializerContext;
