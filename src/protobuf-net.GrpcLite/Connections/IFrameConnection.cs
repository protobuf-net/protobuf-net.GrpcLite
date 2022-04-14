using System;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace ProtoBuf.Grpc.Lite.Connections;

/// <summary>
/// Represents a processor capable of reading and writing <see cref="Frame"/> data.
/// </summary>
public interface IFrameConnection : IDisposable
{
    /// <summary>
    /// Writes all the frames from the provided channel.
    /// </summary>
    Task WriteAsync(ChannelReader<(Frame Frame, FrameWriteFlags Flags)> source, CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads all frames from the underlying source, invoking the supplied action for each.
    /// </summary>
    Task ReadAllAsync(Func<Frame, ValueTask> action, CancellationToken cancellationToken = default);
}