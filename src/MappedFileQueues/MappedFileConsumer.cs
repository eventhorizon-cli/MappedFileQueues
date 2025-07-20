using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace MappedFileQueues;

internal class MappedFileConsumer<T> : IMappedFileConsumer<T>, IDisposable where T : struct
{
    private readonly MappedFileQueueOptions _options;

    // Memory mapped file to store the consumer offset
    private readonly OffsetMappedFile _offsetFile;

    private readonly int _payloadSize;

    private readonly string _segmentDirectory;

    private MappedFileSegment<T>? _segment;

    private bool _disposed;

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

        _payloadSize = Unsafe.SizeOf<T>();

        _segmentDirectory = Path.Combine(options.StorePath, Constants.CommitLogDirectory);
    }

    public long Offset => _offsetFile.Offset;

    public void AdjustOffset(long offset)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (offset < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(offset), "Offset must be greater than or equal to zero.");
        }

        if (_segment != null)
        {
            throw new InvalidOperationException(
                "Cannot adjust offset while there is an active segment. Please adjust the offset before consuming any messages.");
        }

        _offsetFile.MoveTo(offset);
    }

    public void Consume(out T message)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

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

        while (!_segment.TryRead(_offsetFile.Offset, out message))
        {
            // Spin wait until the message is available or timeout
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
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (_segment == null)
        {
            throw new InvalidOperationException(
                $"No matched segment found. Ensure {nameof(Consume)} is called before {nameof(Commit)}.");
        }

        _offsetFile.Advance(_payloadSize + Constants.EndMarkerSize);

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
            _offsetFile.Offset,
            out segment);
}
