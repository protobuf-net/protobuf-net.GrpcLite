using ProtoBuf.Grpc.Lite.Connections;
using System;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace ProtoBuf.Grpc.Lite.Internal.Connections;

internal sealed class NullConnection : IFrameConnection
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

    async Task IFrameConnection.ReadAllAsync(Func<Frame, ValueTask> action, CancellationToken cancellationToken)
    {
        try
        {
            do
            {
                while (_input.TryRead(out var pair))
                {
                    await action(pair.Frame);
                }
            }
            while (await _input.WaitToReadAsync(cancellationToken));
            _output.TryComplete(); // signal the EOF scenario
        }
        catch (Exception ex)
        {
            _output.TryComplete(ex); // try to signal the problem - we're broken
        }
    }

    Task IFrameConnection.WriteAsync(ChannelReader<(Frame Frame, FrameWriteFlags Flags)> source, CancellationToken cancellationToken)
        => source.Completion;
}
