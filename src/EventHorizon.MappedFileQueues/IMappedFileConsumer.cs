namespace EventHorizon.MappedFileQueues;

public interface IMappedFileConsumer<T> : IDisposable where T : struct
{
    public void Consume(out T value);

    public void Commit();
}