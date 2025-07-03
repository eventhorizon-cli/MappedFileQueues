namespace MappedFileQueues;

public sealed class MappedFileQueue<T> : IDisposable where T : struct
{
    private readonly MappedFileQueueOptions _options;

    private MappedFileProducer<T>? _producer;
    private MappedFileConsumer<T>? _consumer;

    public MappedFileQueue(MappedFileQueueOptions options)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(options.StorePath, nameof(options.StorePath));

        if (File.Exists(options.StorePath))
        {
            throw new ArgumentException($"The path '{options.StorePath}' is a file, not a directory.",
                nameof(options.StorePath));
        }

        if (!Directory.Exists(options.StorePath))
        {
            Directory.CreateDirectory(options.StorePath);
        }

        if (options.SegmentSize <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(options.SegmentSize),
                "SegmentSize must be greater than zero.");
        }

        _options = options;
    }

    public IMappedFileProducer<T> Producer => _producer ??= new MappedFileProducer<T>(_options);
    public IMappedFileConsumer<T> Consumer => _consumer ??= new MappedFileConsumer<T>(_options);

    public void Dispose()
    {
        _producer?.Dispose();
        _consumer?.Dispose();
    }
}
