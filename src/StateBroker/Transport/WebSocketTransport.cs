using System.Net;
using System.Runtime.CompilerServices;
using StateBroker.Core;

namespace StateBroker.Transport;

public sealed class WebSocketTransport : ITransport
{
    private readonly HttpListener _listener;

    public WebSocketTransport(string prefix)
    {
        _listener = new HttpListener();
        _listener.Prefixes.Add(prefix.EndsWith('/') ? prefix : prefix + "/");
    }

    public async IAsyncEnumerable<IConnection> AcceptAsync(
        [EnumeratorCancellation] CancellationToken ct)
    {
        _listener.Start();
        try
        {
            while (!ct.IsCancellationRequested)
            {
                HttpListenerContext context;
                try
                {
                    context = await _listener.GetContextAsync().WaitAsync(ct);
                }
                catch (OperationCanceledException)
                {
                    yield break;
                }

                if (!context.Request.IsWebSocketRequest)
                {
                    context.Response.StatusCode = 400;
                    context.Response.Close();
                    continue;
                }

                var clientId = context.Request.QueryString["clientId"]
                    ?? Guid.NewGuid().ToString("N");

                var wsContext = await context.AcceptWebSocketAsync(subProtocol: null);
                yield return new WebSocketConnection(clientId, wsContext.WebSocket);
            }
        }
        finally
        {
            _listener.Stop();
        }
    }

    public ValueTask DisposeAsync()
    {
        _listener.Close();
        return ValueTask.CompletedTask;
    }
}
