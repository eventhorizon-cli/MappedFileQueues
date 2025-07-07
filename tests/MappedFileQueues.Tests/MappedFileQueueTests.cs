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
            SegmentSize = 1024,
            ConsumerRetryInterval = TimeSpan.FromMilliseconds(100),
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

        for (var i = 0; i < 10; i++)
        {
            await Task.Delay(200); // for testing the spin wait in the consumer
            var testStruct = new TestStructSize16 { A = i, B = i + 1, C = i + 2, D = i + 3 };

            producer.Produce(ref testStruct);
        }

        await consumeTask;
    }

    [Fact]
    public void Consume_From_Last_Offset()
    {
        using var tempStorePath = TempStorePath.Create();

        var options = new MappedFileQueueOptions { StorePath = tempStorePath.Path, SegmentSize = 32 };

        using (var queue1 = MappedFileQueue.Create<TestStructSize16>(options))
        {
            var producer1 = queue1.Producer;

            for (var i = 0; i < 10; i++)
            {
                var testStruct = new TestStructSize16 { A = i, B = i + 1, C = i + 2, D = i + 3 };
                producer1.Produce(ref testStruct);
            }

            var consumer1 = queue1.Consumer;

            for (var i = 0; i < 3; i++)
            {
                consumer1.Consume(out var testStruct);
                consumer1.Commit();

                Assert.Equal(i, testStruct.A);
                Assert.Equal(i + 1, testStruct.B);
                Assert.Equal(i + 2, testStruct.C);
                Assert.Equal(i + 3, testStruct.D);
            }
        }

        using (var queue2 = MappedFileQueue.Create<TestStructSize16>(options))
        {
            var consumer2 = queue2.Consumer;

            // Consume from the last offset
            for (var i = 3; i < 10; i++)
            {
                consumer2.Consume(out var testStruct);
                consumer2.Commit();

                Assert.Equal(i, testStruct.A);
                Assert.Equal(i + 1, testStruct.B);
                Assert.Equal(i + 2, testStruct.C);
                Assert.Equal(i + 3, testStruct.D);
            }
        }
    }

    [Fact]
    public void Produce_From_Last_Offset()
    {
        using var tempStorePath = TempStorePath.Create();

        var options = new MappedFileQueueOptions { StorePath = tempStorePath.Path, SegmentSize = 32 };

        using (var queue1 = MappedFileQueue.Create<TestStructSize16>(options))
        {
            var producer1 = queue1.Producer;

            for (var i = 0; i < 10; i++)
            {
                var testStruct = new TestStructSize16 { A = i, B = i + 1, C = i + 2, D = i + 3 };
                producer1.Produce(ref testStruct);
            }
        }

        using (var queue2 = MappedFileQueue.Create<TestStructSize16>(options))
        {
            var producer2 = queue2.Producer;

            for (var i = 10; i < 15; i++)
            {
                var testStruct = new TestStructSize16 { A = i, B = i + 1, C = i + 2, D = i + 3 };
                producer2.Produce(ref testStruct);
            }

            var consumer2 = queue2.Consumer;

            for (var i = 0; i < 15; i++)
            {
                consumer2.Consume(out var testStruct);
                consumer2.Commit();

                Assert.Equal(i, testStruct.A);
                Assert.Equal(i + 1, testStruct.B);
                Assert.Equal(i + 2, testStruct.C);
                Assert.Equal(i + 3, testStruct.D);
            }
        }
    }

    [Fact]
    public void Consumer_Can_Retry_If_Offset_Not_Commited()
    {
        using var tempStorePath = TempStorePath.Create();

        var options = new MappedFileQueueOptions
        {
            StorePath = tempStorePath.Path,
            SegmentSize = 32,
            ConsumerRetryInterval = TimeSpan.FromMilliseconds(100),
            ConsumerSpinWaitDuration = TimeSpan.FromMilliseconds(50)
        };

        using var queue = MappedFileQueue.Create<TestStructSize16>(options);

        var producer = queue.Producer;

        for (var i = 0; i < 5; i++)
        {
            var testStruct = new TestStructSize16 { A = i, B = i + 1, C = i + 2, D = i + 3 };
            producer.Produce(ref testStruct);
        }

        var consumer = queue.Consumer;

        // Consume without committing
        for (var i = 0; i < 5; i++)
        {
            consumer.Consume(out var testStruct);
            Assert.Equal(0, testStruct.A);
            Assert.Equal(1, testStruct.B);
            Assert.Equal(2, testStruct.C);
            Assert.Equal(3, testStruct.D);
        }

        // Consume again, this time committing the offset
        for (var i = 0; i < 5; i++)
        {
            consumer.Consume(out var testStruct);
            consumer.Commit();

            Assert.Equal(i, testStruct.A);
            Assert.Equal(i + 1, testStruct.B);
            Assert.Equal(i + 2, testStruct.C);
            Assert.Equal(i + 3, testStruct.D);
        }
    }
}
