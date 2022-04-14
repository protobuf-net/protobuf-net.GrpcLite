using System;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;

namespace ProtoBuf.Grpc.Lite.Internal
{
    internal class OverlappedMicroChannel<T> : ChannelReader<T>, IValueTaskSource<bool>, IValueTaskSource, IDisposable
    {
        [Flags]
        enum Flags
        {
            None = 0,
            Completed = 1 << 0,
            HaveValue = 1 << 1,
            WaitingReader = 1 << 2,
            WaitingWriter = 1 << 3,
        }
        volatile Flags _flags;
        volatile Exception? _fault;
        T? _value;

        private ManualResetValueTaskSourceCore<bool> _pendingRead, _pendingWrite;
        private CancellationTokenRegistration _cancelReg;
        public CancellationToken CancellationToken { get; }
        private OverlappedMicroChannel(CancellationToken cancellationToken)
        {
            CancellationToken = cancellationToken;
            if (cancellationToken.CanBeCanceled)
            {
                _cancelReg = cancellationToken.Register(s_Cancel, this);
            }
        }

        void IDisposable.Dispose()
        {
            var tmp = _cancelReg;
            _cancelReg = default;
            tmp.Dispose();
        }

#if !NETCOREAPP3_1
        public override bool CanCount => true;
        public override int Count
        {
            get
            {
                lock(this)
                {
                    return (_flags & Flags.HaveValue) != 0 ? 1 : 0;
                }
            }
        }
#endif

#if NET6_0_OR_GREATER || !NETCOREAPP3_1_OR_GREATER
        public override bool CanPeek => true;
        public override bool TryPeek([NotNullWhen(true)] out T? item)
        {
            lock (this)
            {
                if ((_flags & Flags.HaveValue) != 0)
                {
                    item = _value!;
                    return true;
                }
            }
            item = default;
            return false;
        }
#endif

        public static bool Create([NotNull] ref OverlappedMicroChannel<T>? field, CancellationToken cancellationToken)
        {
            if (Volatile.Read(ref field) is not null)
            {
#pragma warning disable CS8777
                return false;
#pragma warning restore CS8777
            }

            var newObj = new OverlappedMicroChannel<T>(cancellationToken);
            var swapped = Interlocked.CompareExchange(ref field, newObj, null) is null;
            if (!swapped) newObj.SafeDispose();
            return swapped;
        }

#pragma warning disable CS8765 // nullability mismatch
        public override bool TryRead([MaybeNullWhen(false)] out T item)
#pragma warning restore CS8765 // nullability mismatch
        {
            lock (this)
            {
                AssertCanReadLocked();
                if ((_flags & Flags.HaveValue) != 0)
                {
                    item = _value!;
                    _value = default;
                    _flags &= ~Flags.HaveValue;

                    ActivateWriterLocked();
                    return true;
                }
            }
            item = default;
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void AssertCanReadLocked()
        {
            if ((_flags & Flags.WaitingReader) != 0) ThrowPending();
            static void ThrowPending() => throw new InvalidOperationException("There is already a pending read operation");
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void AssertCanWriteLocked()
        {
            if ((_flags & Flags.WaitingWriter) != 0) ThrowPending();
            if ((_flags & Flags.Completed) != 0) ThrowCompleted();
            static void ThrowPending() => throw new InvalidOperationException("There is already a pending write operation");
            static void ThrowCompleted() => throw new InvalidOperationException("The channel has already been completed");
        }

        public override ValueTask<bool> WaitToReadAsync(CancellationToken cancellationToken = default)
        {
            lock (this)
            {
                AssertCanReadLocked();
                if ((_flags & Flags.HaveValue) != 0)
                {
                    return new ValueTask<bool>(true);
                }
                if ((_flags & Flags.Completed) != 0)
                {
                    return new ValueTask<bool>(false);
                }
                var ex = _fault;
                if (ex is not null) return FaultedBool(ex);

                cancellationToken.ThrowIfCancellationRequested(); // note we don't fully support per-call cancellation, but if the channel is completed: fine
                _flags |= Flags.WaitingReader;
                return new ValueTask<bool>(this, _pendingRead.Version);
            }
        }

        public bool TryWrite(T value)
        {
            lock (this)
            {
                AssertCanWriteLocked();
                if ((_flags & Flags.HaveValue) == 0)
                {
                    _value = value;
                    _flags |= Flags.HaveValue;

                    ActivateReaderLocked();
                    return true;
                }
                return false;
            }
        }

        public ValueTask WriteToWriteAsync()
        {
            lock (this)
            {
                AssertCanWriteLocked();

                if ((_flags & Flags.HaveValue) == 0) return default;

                var ex = _fault;
                if (ex is not null) return Faulted(ex);
                _flags |= Flags.WaitingWriter;
                return new ValueTask(this, _pendingWrite.Version);
            }
        }

        public bool TryComplete(Exception? fault = null)
        {
            lock (this)
            {
                if ((_flags & Flags.Completed) != 0) return false;

                _fault = fault;
                _flags |= Flags.Completed;

                ActivateReaderLocked();
                ActivateWriterLocked();
                return true;
            }
        }

        private void ActivateReaderLocked()
        {
            if ((_flags & Flags.WaitingReader) != 0)
            {
                _flags &= ~Flags.WaitingReader;
                ThreadPool.UnsafeQueueUserWorkItem(s_ActivateReader, this);
            }
        }

        private void ActivateWriterLocked()
        {
            if ((_flags & Flags.WaitingWriter) != 0)
            {
                _flags &= ~Flags.WaitingWriter;
                ThreadPool.UnsafeQueueUserWorkItem(s_ActivateWriter, this);
            }
        }

        static ValueTask Faulted(Exception ex)
        {
#if NET5_0_OR_GREATER
            return ValueTask.FromException(ex);
#else
            return new ValueTask(Task.FromException(ex));
#endif
        }

        static ValueTask<bool> FaultedBool(Exception ex)
        {
#if NET5_0_OR_GREATER
            return ValueTask.FromException<bool>(ex);
#else
            return new ValueTask<bool>(Task.FromException<bool>(ex));
#endif
        }

        private static readonly WaitCallback s_ActivateReader = state => Unsafe.As<OverlappedMicroChannel<T>>(state!).ActivateReader();
        private static readonly WaitCallback s_ActivateWriter = state => Unsafe.As<OverlappedMicroChannel<T>>(state!).ActivateWriter();
        private static readonly Action<object?> s_Cancel = state => Unsafe.As<OverlappedMicroChannel<T>>(state!).Cancel();

        private void Cancel()
        {
            try
            {
                CancellationToken.ThrowIfCancellationRequested();
                TryComplete();
            }
            catch (Exception ex)
            {
                TryComplete(ex);
            }
        }
        private void ActivateReader()
        {
            try
            {
                var ex = _fault;
                if (ex is not null) _pendingRead.SetException(ex);
                _pendingRead.SetResult((_flags & Flags.Completed) == 0);
            }
            catch (Exception ex)
            {
                Debug.WriteLine(ex);
            }
        }

        private void ActivateWriter()
        {
            try
            {
                var ex = _fault;
                if (ex is not null) _pendingWrite.SetException(ex);
                _pendingWrite.SetResult(true); // dummy value
            }
            catch (Exception ex)
            {
                Debug.WriteLine(ex);
            }
        }

        ValueTaskSourceStatus IValueTaskSource<bool>.GetStatus(short token)
            => _pendingRead.GetStatus(token);

        void IValueTaskSource<bool>.OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags)
            => _pendingRead.OnCompleted(continuation, state, token, flags);

        bool IValueTaskSource<bool>.GetResult(short token)
        {
            lock (this)
            {
                var result = _pendingRead.GetResult(token);
                _pendingRead.Reset();
                return result;
            }
        }

        ValueTaskSourceStatus IValueTaskSource.GetStatus(short token)
            => _pendingWrite.GetStatus(token);

        void IValueTaskSource.OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags)
            => _pendingWrite.OnCompleted(continuation, state, token, flags);

        void IValueTaskSource.GetResult(short token)
        {
            lock (this)
            {
                _pendingWrite.GetResult(token);
                _pendingWrite.Reset();
            }
        }
    }
}
