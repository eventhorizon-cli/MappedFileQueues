using System.IO.MemoryMappedFiles;
using System.Runtime.InteropServices;
using MemoryMappedFileQueue;

namespace EventHorizon.MappedFileQueues;

internal class MappedFileConsumer<T> : IMappedFileConsumer<T> where T : struct
{
    private readonly MappedFileQueueOptions _options;

    // Memory mapped file to store the consumer offset
    private int _offset;
    private readonly MemoryMappedFile _offsetFile;
    private readonly MemoryMappedViewAccessor _offsetAccessor;
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
        _offsetFile = MemoryMappedFile.CreateFromFile(offsetPath, FileMode.OpenOrCreate, null, 4,
            MemoryMappedFileAccess.ReadWrite);

        _offsetAccessor = _offsetFile.CreateViewAccessor(0, 4, MemoryMappedFileAccess.ReadWrite);
        _offsetAccessor.Read(0, out _offset);

        _itemSize = Marshal.SizeOf<T>();

        _segmentDirectory = Path.Combine(options.StorePath, Constants.CommitLogDirectory);
    }

    public void Consume(out T item)
    {
        int retryIntervalMs = 1000;
        int spinWaitTimeoutMs = 100;
        while (_segment == null)
        {
            TryFindSegmentByOffset();
            Thread.Sleep(retryIntervalMs);
        }

        if (_offset > _segment.AllowedEndOffset)
        {
            _segment.Dispose();
            _segment = null;
            Consume(out item);
        }

        var startTicks = DateTime.UtcNow.Ticks;
        while (!_segment.TryRead(_offset, out item))
        {
            if ((DateTime.UtcNow.Ticks - startTicks) / TimeSpan.NanosecondsPerTick > spinWaitTimeoutMs)
            {
                // Spin wait until the item is available or timeout
                Thread.Sleep(retryIntervalMs);
            }
        }
    }

    public void Commit()
    {
        _offset = _offset + _itemSize + 1;
        _offsetAccessor.Write(0, ref _offset);
    }

    public void Dispose()
    {
        _offsetFile.Dispose();
        _offsetAccessor.Dispose();
        _segment?.Dispose();
    }

    private void TryFindSegmentByOffset() =>
        MappedFileSegment<T>.TryCreateOrFindByOffset(
            _segmentDirectory,
            _options.SegmentSize,
            _offset,
            true,
            out _segment);
}