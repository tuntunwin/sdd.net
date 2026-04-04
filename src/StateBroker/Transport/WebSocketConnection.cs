using System.Net.WebSockets;
using System.Runtime.CompilerServices;
using System.Text.Json;
using StateBroker.Core;

namespace StateBroker.Transport;

public sealed class WebSocketConnection : IConnection
{
    private readonly WebSocket _ws;
    private readonly SemaphoreSlim _sendLock = new(1, 1);

    public string ClientId { get; }

    public WebSocketConnection(string clientId, WebSocket ws)
    {
        ClientId = clientId;
        _ws = ws;
    }

    public async IAsyncEnumerable<Frame> ReadFramesAsync(
        [EnumeratorCancellation] CancellationToken ct)
    {
        var buffer = new byte[8192];
        while (_ws.State == WebSocketState.Open && !ct.IsCancellationRequested)
        {
            Frame? frame;
            try
            {
                using var ms = new MemoryStream();
                WebSocketReceiveResult result;
                do
                {
                    result = await _ws.ReceiveAsync(new ArraySegment<byte>(buffer), ct);
                    if (result.MessageType == WebSocketMessageType.Close)
                        yield break;
                    ms.Write(buffer, 0, result.Count);
                } while (!result.EndOfMessage);

                frame = JsonSerializer.Deserialize(
                    ms.ToArray(), BrokerJsonContext.Default.Frame);
            }
            catch (OperationCanceledException)
            {
                yield break;
            }
            catch (WebSocketException)
            {
                yield break;
            }

            if (frame is not null)
                yield return frame;
        }
    }

    public async ValueTask SendAsync(Frame frame, CancellationToken ct)
    {
        var bytes = JsonSerializer.SerializeToUtf8Bytes(frame, BrokerJsonContext.Default.Frame);
        await _sendLock.WaitAsync(ct);
        try
        {
            await _ws.SendAsync(
                new ArraySegment<byte>(bytes),
                WebSocketMessageType.Text,
                endOfMessage: true,
                ct);
        }
        finally
        {
            _sendLock.Release();
        }
    }

    public async ValueTask CloseAsync()
    {
        try
        {
            if (_ws.State is WebSocketState.Open or WebSocketState.CloseReceived)
                await _ws.CloseAsync(
                    WebSocketCloseStatus.NormalClosure, null, CancellationToken.None);
        }
        catch (WebSocketException) { }
    }
}
