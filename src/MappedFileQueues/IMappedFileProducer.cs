namespace MappedFileQueues;

public interface IMappedFileProducer<T> where T : struct
{
    /// <summary>
    /// The next offset where message will be written in the mapped file queue.
    /// </summary>
    public long Offset { get; }

    /// <summary>
    /// The last offset that has been fully persisted (confirmed written to storage).
    /// </summary>
    public long ConfirmedOffset { get; }

    /// <summary>
    /// Adjusts the offset to produce to the mapped file queue.
    /// </summary>
    /// <param name="offset">The new offset to set.</param>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when the provided offset is negative.</exception>
    /// <exception cref="InvalidOperationException">Thrown when the producer has already started producing messages.</exception>
    public void AdjustOffset(long offset);

    /// <summary>
    /// Produces a message to the mapped file queue.
    /// </summary>
    /// <param name="message">The message to produce.</param>
    public void Produce(ref T message);
}
