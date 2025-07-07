namespace MappedFileQueues;

internal class Constants
{
    public const string CommitLogDirectory = "commitlog";

    public const string OffsetDirectory = "offset";

    public const string ProducerOffsetFile = "producer.offset";

    public const string ConsumerOffsetFile = "consumer.offset";

    public const byte EndMarker = 0xFF;

    public const byte EndMarkerSize = 1;
}
