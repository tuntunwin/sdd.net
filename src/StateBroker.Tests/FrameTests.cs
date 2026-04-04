using System.Text.Json;
using StateBroker.Core;

namespace StateBroker.Tests;

public class FrameTests
{
    [Fact]
    public void Defaults_are_correct()
    {
        var frame = new Frame("PUBLISH");

        Assert.Null(frame.Topic);
        Assert.Equal(0, frame.Qos);
        Assert.Null(frame.MsgId);
        Assert.False(frame.Retain);
        Assert.False(frame.Retained);
        Assert.Equal(default, frame.Payload);
    }

    [Fact]
    public void Full_construction_round_trips()
    {
        var payload = JsonDocument.Parse("""{"temp":22.5}""").RootElement;
        var frame = new Frame(
            Type:     Frame.Types.Publish,
            Topic:    "sensors/temp",
            Qos:      1,
            MsgId:    "msg-001",
            Retain:   true,
            Retained: false,
            Payload:  payload);

        Assert.Equal("PUBLISH", frame.Type);
        Assert.Equal("sensors/temp", frame.Topic);
        Assert.Equal(1, frame.Qos);
        Assert.Equal("msg-001", frame.MsgId);
        Assert.True(frame.Retain);
        Assert.False(frame.Retained);
        Assert.Equal(22.5, frame.Payload.GetProperty("temp").GetDouble());
    }

    [Fact]
    public void Record_equality_works()
    {
        var a = new Frame(Frame.Types.Ping);
        var b = new Frame(Frame.Types.Ping);
        Assert.Equal(a, b);

        var c = new Frame(Frame.Types.Pong);
        Assert.NotEqual(a, c);
    }

    [Fact]
    public void With_expression_creates_copy()
    {
        var original = new Frame(Frame.Types.Publish, Topic: "a", Qos: 0);
        var deliver = original with { Type = Frame.Types.Deliver, Retained = true };

        Assert.Equal(Frame.Types.Deliver, deliver.Type);
        Assert.Equal("a", deliver.Topic);
        Assert.True(deliver.Retained);
        Assert.Equal(Frame.Types.Publish, original.Type);
    }

    [Fact]
    public void Type_constants_have_expected_values()
    {
        Assert.Equal("PUBLISH", Frame.Types.Publish);
        Assert.Equal("SUBSCRIBE", Frame.Types.Subscribe);
        Assert.Equal("ACK", Frame.Types.Ack);
        Assert.Equal("DELIVER", Frame.Types.Deliver);
        Assert.Equal("PING", Frame.Types.Ping);
        Assert.Equal("PONG", Frame.Types.Pong);
        Assert.Equal("REDIRECT", Frame.Types.Redirect);
    }

    [Fact]
    public void Json_round_trip_via_source_generator()
    {
        var payload = JsonDocument.Parse("""{"v":1}""").RootElement;
        var frame = new Frame(Frame.Types.Publish, "t/1", 1, "m1", true, false, payload);

        var bytes = JsonSerializer.SerializeToUtf8Bytes(frame, BrokerJsonContext.Default.Frame);
        var back = JsonSerializer.Deserialize(bytes, BrokerJsonContext.Default.Frame)!;

        Assert.Equal(frame.Type, back.Type);
        Assert.Equal(frame.Topic, back.Topic);
        Assert.Equal(frame.Qos, back.Qos);
        Assert.Equal(frame.MsgId, back.MsgId);
        Assert.Equal(frame.Retain, back.Retain);
        Assert.Equal(1, back.Payload.GetProperty("v").GetInt32());
    }
}
