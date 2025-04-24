using System.Runtime.InteropServices;

namespace EventHorizon.MappedFileQueues;

internal class MappedFileConsumer<T> : IMappedFileConsumer<T> where T : struct
{
    private readonly MappedFileQueueOptions _options;

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
        int retryIntervalMs = 1000;
        int spinWaitTimeoutMs = 100;
        while (_segment == null)
        {
            if (!TryFindSegmentByOffset())
            {
                Thread.Sleep(retryIntervalMs);
            }
        }

        if (_offsetFile.Offset > _segment.AllowedLastOffsetToWrite)
        {
            _segment.Dispose();
            _segment = null;
            Consume(out item);
        }


        var spinWait = new SpinWait();
        var startTicks = DateTime.UtcNow.Ticks;

        while (!_segment.TryRead(_offsetFile.Offset, out item))
        {
            if ((DateTime.UtcNow.Ticks - startTicks) / TimeSpan.TicksPerMillisecond > spinWaitTimeoutMs)
            {
                // Spin wait until the item is available or timeout
                Thread.Sleep(retryIntervalMs);
            }

            // Use SpinWait to avoid busy waiting
            spinWait.SpinOnce();
        }
    }

    public void Commit()
    {
        _offsetFile.Advance(_itemSize + 1);
    }

    public void Dispose()
    {
        _offsetFile.Dispose();
        _segment?.Dispose();
    }

    private bool TryFindSegmentByOffset() =>
        MappedFileSegment<T>.TryCreateOrFindByOffset(
            _segmentDirectory,
            _options.SegmentSize,
            _offsetFile.Offset,
            true,
            out _segment);
}