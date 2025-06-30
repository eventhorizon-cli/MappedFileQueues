namespace EventHorizon.MappedFileQueues;

public interface IMappedFileConsumer<T>  where T : struct
{
    public void Consume(out T value);

    public void Commit();
}