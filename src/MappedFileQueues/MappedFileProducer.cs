using System.Runtime.InteropServices;

namespace MappedFileQueues;

internal class MappedFileProducer<T> : IMappedFileProducer<T>, IDisposable where T : struct
{
    private readonly MappedFileQueueOptions _options;

    // Memory mapped file to store the producer offset
    private readonly OffsetMappedFile _offsetFile;

    private readonly int _payloadSize;

    private readonly string _segmentDirectory;

    private MappedFileSegment<T>? _segment;

    private bool _disposed;

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

        _payloadSize = Marshal.SizeOf<T>();

        _segmentDirectory = Path.Combine(options.StorePath, Constants.CommitLogDirectory);
    }

    public long Offset => _offsetFile.Offset;

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
