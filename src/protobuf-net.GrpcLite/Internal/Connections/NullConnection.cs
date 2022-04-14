using ProtoBuf.Grpc.Lite.Connections;
using System;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace ProtoBuf.Grpc.Lite.Internal.Connections;

internal sealed class NullConnection : ChannelReader<Frame>, IFrameConnection
{
    private readonly ChannelReader<(Frame Frame, FrameWriteFlags Flags)> _input;
    private readonly ChannelWriter<(Frame Frame, FrameWriteFlags Flags)> _output;

    public ChannelWriter<(Frame Frame, FrameWriteFlags Flags)> Output => _output;

    internal static void CreateLinkedPair(out IFrameConnection x, out IFrameConnection y)
    {
        var a = Channel.CreateUnbounded<(Frame Frame, FrameWriteFlags Flags)>(Utilities.UnboundedChannelOptions_SingleReadMultiWriterNoSync);
        var b = Channel.CreateUnbounded<(Frame Frame, FrameWriteFlags Flags)>(Utilities.UnboundedChannelOptions_SingleReadMultiWriterNoSync);

        x = new NullConnection(a.Reader, b.Writer);
        y = new NullConnection(b.Reader, a.Writer);
    }
    public NullConnection(ChannelReader<(Frame Frame, FrameWriteFlags Flags)> input, ChannelWriter<(Frame Frame, FrameWriteFlags Flags)> output)
    {
        _input = input;
        _output = output;
    }

    void IDisposable.Dispose()
    {
        _output.TryComplete();
    }

    ChannelReader<Frame> IFrameConnection.ReadAsync(CancellationToken cancellationToken) => this;

#if !NETCOREAPP3_1
    public override bool CanCount => _input.CanCount;
    public override int Count => _input.Count;
#endif

#if NET6_0_OR_GREATER || !NETCOREAPP3_1_OR_GREATER
    public override bool CanPeek => _input.CanPeek;
    public override bool TryPeek(out Frame item)
    {
        if (_input.TryPeek(out var pair))
        {
            item = pair.Frame;
            return true;
        }
        item = default;
        return false;
    }
#endif

    public override Task Completion => _input.Completion;
    public override bool TryRead([MaybeNullWhen(false)] out Frame item)
    {
        if (_input.TryRead(out var pair))
        {
            item = pair.Frame;
            return true;
        }
        item = default;
        return false;
    }

    public override ValueTask<bool> WaitToReadAsync(CancellationToken cancellationToken = default)
        => _input.WaitToReadAsync(cancellationToken);

    Task IFrameConnection.WriteAsync(ChannelReader<(Frame Frame, FrameWriteFlags Flags)> source, CancellationToken cancellationToken)
        => source.Completion;
}
