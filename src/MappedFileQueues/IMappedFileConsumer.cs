namespace MappedFileQueues;

public interface IMappedFileConsumer<T> where T : struct
{
    /// <summary>
    /// The next offset to consume from the mapped file queue.
    /// </summary>
    public long Offset { get; }

    /// <summary>
    /// Adjusts the offset to consume from the mapped file queue.
    /// </summary>
    /// <param name="offset">The new offset to set.</param>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when the provided offset is negative.</exception>
    /// <exception cref="InvalidOperationException">Thrown when the producer has already started consuming messages.</exception>
    public void AdjustOffset(long offset);

    /// <summary>
    /// Consumes a message from the mapped file queue.
    /// </summary>
    /// <param name="message">The consumed message will be written to this out parameter.</param>
    public void Consume(out T message);

    /// <summary>
    /// Commits the offset of the last consumed message.
    /// </summary>
    public void Commit();

    /// <summary>
    /// Checks if there is a next message available to consume.
    /// </summary>
    /// <returns>True if there is a next message available; otherwise, false.</returns>
    internal bool NextMessageAvailable();
}
