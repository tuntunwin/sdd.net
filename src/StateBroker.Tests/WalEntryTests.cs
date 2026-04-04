using System.Text.Json;
using StateBroker.Core;

namespace StateBroker.Tests;

public class WalEntryTests
{
    [Fact]
    public void Construction_and_properties()
    {
        var payload = JsonDocument.Parse("""{"on":true}""").RootElement;
        var entry = new WalEntry(42, "lights/kitchen", payload);

        Assert.Equal(42, entry.Seq);
        Assert.Equal("lights/kitchen", entry.Topic);
        Assert.True(entry.Payload.GetProperty("on").GetBoolean());
    }

    [Fact]
    public void Json_round_trip()
    {
        var payload = JsonDocument.Parse("123").RootElement;
        var entry = new WalEntry(1, "counter", payload);

        var bytes = JsonSerializer.SerializeToUtf8Bytes(entry, BrokerJsonContext.Default.WalEntry);
        var back = JsonSerializer.Deserialize(bytes, BrokerJsonContext.Default.WalEntry)!;

        Assert.Equal(entry.Seq, back.Seq);
        Assert.Equal(entry.Topic, back.Topic);
        Assert.Equal(123, back.Payload.GetInt32());
    }
}
