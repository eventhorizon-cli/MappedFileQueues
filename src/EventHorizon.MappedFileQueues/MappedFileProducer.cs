using System.Runtime.InteropServices;

namespace EventHorizon.MappedFileQueues;

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
        if (_disposed)
        {
            throw new ObjectDisposedException(nameof(MappedFileProducer<T>));
        }

        if (_segment == null)
        {
            // Initialize the first segment
            OpenOrCreateSegmentByOffset();
        }

        // The first segment is initialized in the constructor
        while (_segment.AllowedLastOffsetToWrite < _offsetFile.Offset)
        {
            // Not enough space in the current segment, create a new one
            _segment.Dispose();
            OpenOrCreateSegmentByOffset();
        }

        _segment.Write(_offsetFile.Offset, ref item);

        Commit();
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }
        _offsetFile.Dispose();
        _segment?.Dispose();
    }

    private void Commit()
    {
        _offsetFile.Advance(_itemSize + 1);
    }

    private void OpenOrCreateSegmentByOffset()
    {
        MappedFileSegment<T>.TryCreateOrFindByOffset(
            _segmentDirectory,
            _options.SegmentSize,
            _offsetFile.Offset,
            false,
            out _segment);
    }
}