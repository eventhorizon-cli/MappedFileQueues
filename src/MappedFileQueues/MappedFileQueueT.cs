namespace MappedFileQueues;

public sealed class MappedFileQueue<T> : IDisposable where T : struct
{
    private readonly MappedFileQueueOptions _options;

    private MappedFileProducer<T>? _producer;
    private MappedFileConsumer<T>? _consumer;

    public MappedFileQueue(MappedFileQueueOptions options)
    {
        _options = options;

        ArgumentException.ThrowIfNullOrWhiteSpace(options.StorePath, nameof(options.StorePath));

        if (options.SegmentSize <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(options.SegmentSize),
                "SegmentSize must be greater than zero.");
        }

        if (File.Exists(options.StorePath))
        {
            throw new ArgumentException($"The path '{options.StorePath}' is a file, not a directory.",
                nameof(options.StorePath));
        }

        if (Directory.Exists(options.StorePath))
        {
            RecoverProducerOffsetIfNeeded();
        }
        else
        {
            Directory.CreateDirectory(options.StorePath);
        }
    }

    public IMappedFileProducer<T> Producer => _producer ??= new MappedFileProducer<T>(_options);
    public IMappedFileConsumer<T> Consumer => _consumer ??= new MappedFileConsumer<T>(_options);

    public void Dispose()
    {
        _producer?.Dispose();
        _consumer?.Dispose();
    }

    // Check the last data. If the producer's offset is greater than the consumer's offset,
    // and the consumer cannot consume the next piece of data, it means that there is data
    // in the queue that was not persisted before the crash. We need to roll back the
    // producer's offset to the position of the data that has been confirmed to be persisted.
    private void RecoverProducerOffsetIfNeeded()
    {
        var lockName = "recovery_lock";
        var processLock = new CrossPlatformProcessLock(lockName, _options.StorePath);

        try
        {
            processLock.Acquire();

            var consumer = Consumer;

            if (consumer.NextMessageAvailable())
            {
                return;
            }

            var producer = Producer;

            if (producer.Offset <= consumer.Offset)
            {
                // the consumer can continue to consume when
                // the producer produces a new message
                return;
            }

            var rollbackOffset = Math.Max(consumer.Offset, producer.ConfirmedOffset);

            if (producer.Offset > rollbackOffset)
            {
                producer.AdjustOffset(rollbackOffset);
            }

            if (producer.Offset > consumer.Offset
                && !consumer.NextMessageAvailable())
            {
                throw new InvalidOperationException(
                    "After recovering the producer's offset, the consumer still cannot consume the next message, the data may be corrupted.");
            }
        }
        finally
        {
            // The producer and consumer may not be used after this recovery process,
            // so we dispose them here.
            _producer?.Dispose();
            _consumer?.Dispose();
            _producer = null;
            _consumer = null;

            processLock.Release();
        }
    }
}
