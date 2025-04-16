namespace EventHorizon.MappedFileQueues;

public interface IMappedFileProducer<T> : IDisposable where T : struct
{
    public void Produce(ref T item);
}