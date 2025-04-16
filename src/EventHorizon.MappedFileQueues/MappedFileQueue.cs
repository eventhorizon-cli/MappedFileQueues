namespace EventHorizon.MappedFileQueues;

public static class MappedFileQueue
{
    public static MappedFileQueueT<T> Create<T>(MappedFileQueueOptions options) where T : struct
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

        return new MappedFileQueueT<T>(options);
    }
}