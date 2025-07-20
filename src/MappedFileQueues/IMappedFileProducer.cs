namespace MappedFileQueues;

public interface IMappedFileProducer<T> where T : struct
{
    /// <summary>
    /// The next offset where message will be written in the mapped file queue.
    /// </summary>
    public long Offset { get; }

    /// <summary>
    /// Produces a message to the mapped file queue.
    /// </summary>
    /// <param name="message">The message to produce.</param>
    public void Produce(ref T message);
}
