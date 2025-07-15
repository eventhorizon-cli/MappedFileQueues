namespace MappedFileQueues;

public interface IMappedFileConsumer<T> where T : struct
{
    public long NextOffset { get; }

    public void Consume(out T value);

    public void Commit();
}
