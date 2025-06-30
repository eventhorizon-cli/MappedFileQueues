namespace EventHorizon.MappedFileQueues;

public interface IMappedFileProducer<T> where T : struct
{
    public void Produce(ref T item);
}