using System.Text.Json;
using StateBroker.Core;

namespace StateBroker.Tests;

public class StateEntryTests
{
    [Fact]
    public void Set_entry()
    {
        var payload = JsonDocument.Parse("""{"x":1}""").RootElement;
        var entry = new StateEntry("topic/a", payload, StateOp.Set);

        Assert.Equal("topic/a", entry.Topic);
        Assert.Equal(StateOp.Set, entry.Op);
        Assert.Equal(1, entry.Payload.GetProperty("x").GetInt32());
    }

    [Fact]
    public void Delete_entry()
    {
        var entry = new StateEntry("topic/b", default, StateOp.Delete);

        Assert.Equal(StateOp.Delete, entry.Op);
        Assert.Equal(default, entry.Payload);
    }

    [Fact]
    public void Json_round_trip()
    {
        var payload = JsonDocument.Parse("""{"y":2}""").RootElement;
        var entry = new StateEntry("t", payload, StateOp.Set);

        var bytes = JsonSerializer.SerializeToUtf8Bytes(entry, BrokerJsonContext.Default.StateEntry);
        var back = JsonSerializer.Deserialize(bytes, BrokerJsonContext.Default.StateEntry)!;

        Assert.Equal(entry.Topic, back.Topic);
        Assert.Equal(entry.Op, back.Op);
        Assert.Equal(2, back.Payload.GetProperty("y").GetInt32());
    }

    [Fact]
    public void Array_json_round_trip()
    {
        var p1 = JsonDocument.Parse("1").RootElement;
        var p2 = JsonDocument.Parse("2").RootElement;
        var arr = new[] {
            new StateEntry("a", p1, StateOp.Set),
            new StateEntry("b", p2, StateOp.Delete),
        };

        var bytes = JsonSerializer.SerializeToUtf8Bytes(arr, BrokerJsonContext.Default.StateEntryArray);
        var back = JsonSerializer.Deserialize(bytes, BrokerJsonContext.Default.StateEntryArray)!;

        Assert.Equal(2, back.Length);
        Assert.Equal("a", back[0].Topic);
        Assert.Equal(StateOp.Delete, back[1].Op);
    }

    [Fact]
    public void StateOp_enum_values()
    {
        Assert.Equal(0, (int)StateOp.Set);
        Assert.Equal(1, (int)StateOp.Delete);
    }
}
