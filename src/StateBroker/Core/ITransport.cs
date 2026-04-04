namespace StateBroker.Core;

public interface ITransport : IAsyncDisposable
{
    IAsyncEnumerable<IConnection> AcceptAsync(CancellationToken ct);
}

public interface IConnection
{
    string ClientId { get; }

    IAsyncEnumerable<Frame> ReadFramesAsync(CancellationToken ct);

    ValueTask SendAsync(Frame frame, CancellationToken ct);

    ValueTask CloseAsync();
}
