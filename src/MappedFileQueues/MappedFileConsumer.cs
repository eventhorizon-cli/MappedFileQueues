using System.Diagnostics.CodeAnalysis;
using System.Runtime.InteropServices;

namespace MappedFileQueues;

internal class MappedFileConsumer<T> : IMappedFileConsumer<T>, IDisposable where T : struct
{
    private readonly MappedFileQueueOptions _options;
    private bool _disposed;

    // Memory mapped file to store the consumer offset
    private readonly OffsetMappedFile _offsetFile;

    private readonly int _itemSize;

    private readonly string _segmentDirectory;
    private MappedFileSegment<T>? _segment;

    public MappedFileConsumer(MappedFileQueueOptions options)
    {
        _options = options;

        var offsetDir = Path.Combine(options.StorePath, Constants.OffsetDirectory);
        if (!Directory.Exists(offsetDir))
        {
            Directory.CreateDirectory(offsetDir);
        }

        var offsetPath = Path.Combine(offsetDir, Constants.ConsumerOffsetFile);
        _offsetFile = new OffsetMappedFile(offsetPath);

        _itemSize = Marshal.SizeOf<T>();

        _segmentDirectory = Path.Combine(options.StorePath, Constants.CommitLogDirectory);
    }

    public void Consume(out T item)
    {
        if (_disposed)
        {
            throw new ObjectDisposedException(nameof(MappedFileConsumer<T>));
        }

        var retryIntervalMs = (int)_options.ConsumerRetryInterval.TotalMilliseconds;
        var spinWaitDurationMs = (int)_options.ConsumerSpinWaitDuration.TotalMilliseconds;

        while (_segment == null)
        {
            if (!TryFindSegmentByOffset(out _segment))
            {
                Thread.Sleep(retryIntervalMs);
            }
        }

        var spinWait = new SpinWait();
        var startTicks = DateTime.UtcNow.Ticks;

        while (!_segment.TryRead(_offsetFile.Offset, out item))
        {
            // Spin wait until the item is available or timeout
            if ((DateTime.UtcNow.Ticks - startTicks) / TimeSpan.TicksPerMillisecond > spinWaitDurationMs)
            {
                // Sleep for a short interval before retrying if spin wait times out
                Thread.Sleep(retryIntervalMs);
            }

            // Use SpinWait to avoid busy waiting
            spinWait.SpinOnce();
        }
    }

    public void Commit()
    {
        if (_disposed)
        {
            throw new ObjectDisposedException(nameof(MappedFileConsumer<T>));
        }

        if (_segment == null)
        {
            throw new InvalidOperationException(
                $"No matched segment found. Ensure {nameof(Consume)} is called before {nameof(Commit)}.");
        }

        _offsetFile.Advance(_itemSize + 1);

        // Check if the segment is fully consumed
        if (_offsetFile.Offset > _segment.AllowedLastOffsetToWrite)
        {
            _segment.Dispose();
            _segment = null;
        }
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        _offsetFile.Dispose();
        _segment?.Dispose();
    }

    private bool TryFindSegmentByOffset([MaybeNullWhen(false)] out MappedFileSegment<T> segment) =>
        MappedFileSegment<T>.TryFind(
            _segmentDirectory,
            _options.SegmentSize,
            _offsetFile.Offset, out segment);
}
