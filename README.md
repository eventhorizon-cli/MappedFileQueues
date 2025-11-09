MappedFileQueues
=================

[![codecov](https://codecov.io/gh/eventhorizon-cli/MappedFileQueues/graph/badge.svg?token=GYTOIKCXD5)](https://codecov.io/gh/eventhorizon-cli/MappedFileQueues)
[![Nuget](https://img.shields.io/nuget/v/MappedFileQueues)](https://www.nuget.org/packages/MappedFileQueues/)

English | [简体中文](./README.zh-CN.md)

**MappedFileQueues** is a high-performance queue based on memory-mapped files, designed specifically for inter-process communication (IPC).

To maximize data transfer efficiency, MappedFileQueues only supports passing value types. If you need to transmit custom reference type data, refer to the project [MappedFileQueues.Stream](https://github.com/eventhorizon-cli/MappedFileQueues.Stream).

### Design Overview

MappedFileQueues persistently store data through memory-mapped files. The overall structure is divided into several Segments, and each Segment contains multiple Messages.

- **Message**: Each message consists of a Payload and an EndMarker.
- **Segment**: The size of a Segment is configurable. The system will automatically adjust the actual size of the Segment so that it does not exceed the configured SegmentSize and can accommodate an integer number of Messages.

![Segment Structure Diagram](./docs/assets/segment.png)

The filename of each Segment is the offset of the first Message in that segment, padded to 20 digits with leading zeros. For example, `0000000000536870912` indicates that the Segment starts at offset 536870912.

- Messages are written and counted by bytes; each time 1 byte is written, the offset increases by 1.
- For example, if the offset is 1024, it means that 1024 bytes of data have been written before.

The offset is stored using the long type, with a maximum supported value of 2^63-1.

To keep the design simple, MappedFileQueues does not handle offset overflow issues. Theoretically, the maximum amount of data that can be written is 2^63-1 bytes (about 8 EB). In practical applications, this limit is unlikely to be reached.

If you really need to handle more data than this limit, consider periodically changing the StorePath or using multiple instances of MappedFileQueues to distribute data.

For performance reasons, when there is no data available to consume, the Consumer will spin-wait first. The maximum duration for a single spin-wait can be set through the configuration option ConsumerSpinWaitDuration, which defaults to 100 milliseconds. If the timeout is reached and no data is available, the consumer will enter sleep state. The sleep duration is controlled by ConsumerRetryInterval, which defaults to 1 second.

### Storage Directory

Under the storage path specified by the `StorePath` configuration option, MappedFileQueues will create the following directory structure:

```bash
├── commitlog
│   ├── 000000000000000000000
│   ├── 000000000000000001024
│   └── ...
├── offset
│   ├── producer.offset
│   ├── producer.confirmed.offset
│   └── consumer.offset
```

Details:

- The `commitlog` directory stores the actual Segment files.

- The `offset` directory stores the offset files for both the producer and the consumer.
  - The `producer.offset` file is used to record the write offset of the producer.
  - The `producer.confirmed.offset` file is used to record the offset that the producer has confirmed to be written to disk. This is used so that after a system abnormal restart, the producer can continue writing data from this offset, avoiding the situation where the consumer waits for lost data.
  - The `consumer.offset` file is used to record the consumption offset of the consumer.

### Usage Example

#### Configuration Options (MappedFileQueueOptions)

- **StorePath**: The storage path, must be a valid folder path.

- **SegmentSize**: The size of each Segment. The system will automatically adjust the actual size so that it does not exceed the configured SegmentSize and can accommodate an integer number of Messages.

- **ConsumerRetryInterval**: The interval for the consumer to retry when there is no data to consume, default is 1 second.

- **ConsumerSpinWaitDuration**: The maximum duration for a single spin-wait for data by the consumer, default is 100 milliseconds.

- **ProducerForceFlushIntervalCount**: The number of messages after which the producer will forcibly flush data to disk after writing. The default is 1000 messages. If this number is not reached, data may be temporarily stored in memory, posing a risk of data loss until the system automatically flushes it to disk. Setting this value to 1 maximizes data safety but may impact performance. In cases of sudden power loss or other exceptions, data that has not been promptly written to disk may be lost. During recovery, the producer's offset will be rolled back to ensure that the consumer does not wait for lost data.

#### Producing and Consuming Data

The producer and consumer interfaces in MappedFileQueues are as follows:

**Note: In actual usage, use a separate thread for LongRunning to consume data in order to avoid occupying thread pool threads.**

```csharp
public interface IMappedFileProducer<T> where T : struct
{
    // Observes the next writable offset for the current producer
    public long Offset { get; }

    // Observes the offset that the current producer has confirmed to be written to disk
    public long ConfirmedOffset { get; }

    // Adjusts the offset for the current producer
    public void AdjustOffset(long offset);

    public void Produce(ref T item);
}

public interface IMappedFileConsumer<T> where T : struct
{
    // Observes the next offset to consume for the current consumer
    public long Offset { get; }

    // Adjusts the offset for the current consumer
    public void AdjustOffset(long offset);

    public T Consume();

    public void Commit();
}
```

Here is a simple usage example:

Define a struct:

```csharp
public unsafe struct TestStruct
{
    public int IntValue;
    public long LongValue;
    public double DoubleValue;
    public fixed char StringValue[20]; // Supports up to 20 characters
}
```

Create a MappedFileQueues instance to get singleton producer and consumer, and produce/consume data:

```csharp
var storePath = "test";

// If you have run the test before, delete the previous data first
if (Directory.Exists(storePath))
{
    Directory.Delete(storePath, true);
}

var queue = MappedFileQueue.Create<TestStruct>(new MappedFileQueueOptions
{
    StorePath = storePath, SegmentSize = 512 * 1024 * 1024 // 512 MB
});

var producer = queue.Producer;

var consumer = queue.Consumer;

var produceTask = Task.Run(() =>
{
    for (var i = 1; i <= 100; i++)
    {
        var testStruct = new TestStruct { IntValue = i, LongValue = i * 10, DoubleValue = i / 2.0 };

        // If you want to use strings in the struct, you can use the following method to copy to the fixed array
        var testString = "TestString_" + i;
        unsafe
        {
            fixed (char* fixedChar = testString)
            {
                Unsafe.CopyBlock(testStruct.StringValue, fixedChar, sizeof(char) * (uint)testString.Length);
            }
        }

        producer.Produce(ref testStruct);
    }

    Console.WriteLine("Produced 100 items.");
});

var consumeTask = Task.Run(() =>
{
    for (var i = 1; i <= 100; i++)
    {
        consumer.Consume(out var testStruct);
        Console.WriteLine(
            $"Consumed: IntValue={testStruct.IntValue}, LongValue={testStruct.LongValue}, DoubleValue={testStruct.DoubleValue}");

        // If you want to use strings in the struct, you can convert the fixed array back to a managed string as follows
        unsafe
        {
            string? managedString = ToManagedString(testStruct.StringValue, 20);
            Console.WriteLine($"StringValue: {managedString}");
        }

        consumer.Commit();
    }

    Console.WriteLine("Consumed 100 items.");
});

await Task.WhenAll(produceTask, consumeTask);


// If you want to use strings in the struct, you can convert the fixed array back to a managed string as follows
unsafe string? ToManagedString(char* source, int maxLength)
{
    if (source == null)
    {
        return null;
    }

    int length = 0;
    while (length < maxLength && source[length] != '\0')
    {
        length++;
    }

    return new string(source, 0, length);
}
```
