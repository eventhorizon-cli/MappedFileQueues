namespace MappedFileQueues;

public static class MappedFileQueue
{
    public static MappedFileQueue<T> Create<T>(MappedFileQueueOptions options) where T : struct => new(options);
}
