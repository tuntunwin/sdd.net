using System.Threading.Channels;

namespace StateBroker.Core;

public sealed class Session : IDisposable
{
    public string ClientId { get; }
    public Channel<Frame> SendChannel { get; }

    private readonly CancellationTokenSource _cts = new();

    public CancellationToken CancellationToken => _cts.Token;

    public Session(string clientId, int sendBufferDepth = 256)
    {
        ClientId = clientId;
        SendChannel = Channel.CreateBounded<Frame>(new BoundedChannelOptions(sendBufferDepth)
        {
            FullMode = BoundedChannelFullMode.DropOldest,
            SingleReader = true,
            SingleWriter = false,
        });
    }

    public bool TrySend(Frame frame) => SendChannel.Writer.TryWrite(frame);

    public void Cancel()
    {
        if (!_cts.IsCancellationRequested)
            _cts.Cancel();
    }

    public void Dispose()
    {
        Cancel();
        SendChannel.Writer.TryComplete();
        _cts.Dispose();
    }
}
