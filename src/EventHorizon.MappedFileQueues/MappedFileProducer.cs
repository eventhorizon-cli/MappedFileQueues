using System.IO.MemoryMappedFiles;
using System.Runtime.InteropServices;
using MemoryMappedFileQueue;

namespace EventHorizon.MappedFileQueues;

internal class MappedFileProducer<T> : IMappedFileProducer<T> where T : struct
{
    private readonly MappedFileQueueOptions _options;

    // Memory mapped file to store the producer offset
    private int _offset;
    private readonly MemoryMappedFile _offsetFile;
    private readonly MemoryMappedViewAccessor _offsetAccessor;
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
        _offsetFile = MemoryMappedFile.CreateFromFile(offsetPath, FileMode.OpenOrCreate, null, 4,
            MemoryMappedFileAccess.ReadWrite);

        _offsetAccessor = _offsetFile.CreateViewAccessor(0, 4, MemoryMappedFileAccess.ReadWrite);
        _offsetAccessor.Read(0, out _offset);

        _itemSize = Marshal.SizeOf<T>();

        _segmentDirectory = Path.Combine(options.StorePath, Constants.CommitLogDirectory);

        // Initialize the first segment
        OpenOrCreateSegmentByOffset();
    }

    public void Produce(ref T item)
    {
        // The first segment is initialized in the constructor
        while (_segment.AllowedEndOffset < _offset)
        {
            _segment.Dispose();
            // Not enough space in the current segment, create a new one
            OpenOrCreateSegmentByOffset();
        }

        _segment.Write(_offset, ref item);

        Commit();
    }

    public void Dispose()
    {
        _offsetFile.Dispose();
        _offsetAccessor.Dispose();
        _segment?.Dispose();
    }

    private void Commit()
    {
        _offset = _offset + _itemSize + 1;
        _offsetAccessor.Write(0, _offset);
    }

    private void OpenOrCreateSegmentByOffset()
    {
        MappedFileSegment<T>.TryCreateOrFindByOffset(
            _segmentDirectory,
            _options.SegmentSize,
            _offset,
            false,
            out _segment);
    }
}