namespace MappedFileQueues;

public interface IMappedFileProducer<T> where T : struct
{
    public long NextOffset { get; }

    public void Produce(ref T item);
}
