namespace EventHorizon.MappedFileQueues;

internal class Constants
{
    public const byte MagicByte = 0xFF;

    public const string CommitLogDirectory = "commitlog";

    public const string OffsetDirectory = "offset";
    
    public const string ProducerOffsetFile = "producer.offset";

    public const string ConsumerOffsetFile = "consumer.offset";
}