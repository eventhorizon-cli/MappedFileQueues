using MappedFileQueues.Tests.TestStructs;

namespace MappedFileQueues.Tests;

public class MappedFileQueueTests
{
    [Fact]
    public void Produce_Then_Consume()
    {
        using var tempStorePath = TempStorePath.Create();

        var options = new MappedFileQueueOptions
        {
            StorePath = tempStorePath.Path,
            SegmentSize = 33 // will be 32 bytes per segment
        };

        using var queue = MappedFileQueue.Create<TestStructSize16>(options);

        var producer = queue.Producer;

        var consumer = queue.Consumer;

        for (var i = 0; i < 10; i++)
        {
            var testStruct = new TestStructSize16 { A = i, B = i + 1, C = i + 2, D = i + 3 };

            producer.Produce(ref testStruct);
        }

        for (var i = 0; i < 10; i++)
        {
            consumer.Consume(out var testStruct);
            consumer.Commit();

            Assert.Equal(i, testStruct.A);
            Assert.Equal(i + 1, testStruct.B);
            Assert.Equal(i + 2, testStruct.C);
            Assert.Equal(i + 3, testStruct.D);
        }
    }

    [Fact]
    public async Task Consume_Then_Produce()
    {
        using var tempStorePath = TempStorePath.Create();

        var options = new MappedFileQueueOptions
        {
            StorePath = tempStorePath.Path,
            SegmentSize = 33, // will be 32 bytes per segment
            ConsumerRetryInterval = TimeSpan.FromMilliseconds(120),
            ConsumerSpinWaitDuration = TimeSpan.FromMilliseconds(50)
        };

        using var queue = MappedFileQueue.Create<TestStructSize16>(options);

        var consumeTask = Task.Run(() =>
        {
            var consumer = queue.Consumer;

            for (var i = 0; i < 10; i++)
            {
                consumer.Consume(out var testStruct);
                consumer.Commit();

                Assert.Equal(i, testStruct.A);
                Assert.Equal(i + 1, testStruct.B);
                Assert.Equal(i + 2, testStruct.C);
                Assert.Equal(i + 3, testStruct.D);
            }
        });

        var producer = queue.Producer;

        // for (var i = 0; i < 10; i++)
        // {
        //     await Task.Delay(100); // for testing the spin wait in the consumer
        //     var testStruct = new TestStructSize16 { A = i, B = i + 1, C = i + 2, D = i + 3 };
        //
        //     producer.Produce(ref testStruct);
        // }
        //
        await consumeTask;
    }
}
