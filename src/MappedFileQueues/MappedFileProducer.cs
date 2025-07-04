using System.Runtime.InteropServices;

namespace MappedFileQueues;

internal class MappedFileProducer<T> : IMappedFileProducer<T>, IDisposable where T : struct
{
    private readonly MappedFileQueueOptions _options;
    private bool _disposed;

    // Memory mapped file to store the producer offset
    private readonly OffsetMappedFile _offsetFile;

    private readonly int _itemSize;

    private readonly string _segmentDirectory;
    private MappedFileSegment<T>? _segment;

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

        _itemSize = Marshal.SizeOf<T>();

        _segmentDirectory = Path.Combine(options.StorePath, Constants.CommitLogDirectory);
    }

    public void Produce(ref T item)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        _segment ??= FindOrCreateSegmentByOffset();

        _segment.Write(_offsetFile.Offset, ref item);

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

        _offsetFile.Advance(_itemSize + 1);

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
