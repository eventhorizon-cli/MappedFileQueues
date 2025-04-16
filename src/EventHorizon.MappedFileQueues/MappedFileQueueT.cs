namespace EventHorizon.MappedFileQueues;

public sealed class MappedFileQueueT<T>(MappedFileQueueOptions options) where T : struct
{
    public IMappedFileProducer<T> CreateProducer()
    {
        return new MappedFileProducer<T>(options);
    }

    public IMappedFileConsumer<T> CreateConsumer()
    {
        return new MappedFileConsumer<T>(options);
    }
}