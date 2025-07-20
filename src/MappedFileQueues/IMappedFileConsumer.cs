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
}
