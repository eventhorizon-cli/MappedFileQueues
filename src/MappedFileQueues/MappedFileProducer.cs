using System.Runtime.CompilerServices;

namespace MappedFileQueues;

internal class MappedFileProducer<T> : IMappedFileProducer<T>, IDisposable where T : struct
{
    private readonly MappedFileQueueOptions _options;

    // Memory mapped file to store the producer offset
    private readonly OffsetMappedFile _offsetFile;

    // Memory mapped file to store the confirmed producer offset
    private readonly OffsetMappedFile _confirmedOffsetFile;

    private readonly int _payloadSize;

    private readonly string _segmentDirectory;

    private MappedFileSegment<T>? _segment;

    private bool _disposed;

    private long _producedCount = 0;

    public MappedFileProducer(MappedFileQueueOptions options)
    {
        _options = options;

        var offsetDir = Path.Combine(options.StorePath, Constants.OffsetDirectory);
        if (!Directory.Exists(offsetDir))
        {
            Directory.CreateDirectory(offsetDir);
        }

        var offsetPath = Path.Combine(offsetDir, Constants.ProducerOffsetFile);
        _offsetFile = new OffsetMappedFile(offsetPath);

        var confirmedOffsetPath = Path.Combine(offsetDir, Constants.ProducerConfirmedOffsetFile);
        _confirmedOffsetFile = new OffsetMappedFile(confirmedOffsetPath);

        _payloadSize = Unsafe.SizeOf<T>();

        _segmentDirectory = Path.Combine(options.StorePath, Constants.CommitLogDirectory);
    }

    public long Offset => _offsetFile.Offset;

    public long ConfirmedOffset => _confirmedOffsetFile.Offset;

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
                "Cannot adjust offset while there is an active segment. Please adjust the offset before producing any messages.");
        }

        _offsetFile.MoveTo(offset, true);
    }

    public void Produce(ref T message)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        _segment ??= FindOrCreateSegmentByOffset();

        _segment.Write(_offsetFile.Offset, ref message);

        Commit();
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        _offsetFile.Dispose();
        _confirmedOffsetFile.Dispose();
        _segment?.Dispose();
    }

    private void Commit()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (_segment == null)
        {
            throw new InvalidOperationException("Segment is not initialized.");
        }

        _offsetFile.Advance(_payloadSize + Constants.EndMarkerSize);

        _producedCount++;

        if (_producedCount % _options.ProducerForceFlushIntervalCount == 0)
        {
            // Force flush the segment and update the confirmed offset
            _segment.Flush();
            _confirmedOffsetFile.MoveTo(_offsetFile.Offset, true);
        }

        // Check if the segment has reached its limit
        if (_segment.AllowedLastOffsetToWrite < _offsetFile.Offset)
        {
            // Dispose the current segment and will create a new one on the next Produce call
            _segment.Dispose();
            _segment = null;
        }
    }

    private MappedFileSegment<T> FindOrCreateSegmentByOffset() =>
        MappedFileSegment<T>.FindOrCreate(
            _segmentDirectory,
            _options.SegmentSize,
            _offsetFile.Offset);
}
