using System.Runtime.CompilerServices;

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

    [Fact]
    public void Consumer_Offset_Can_Be_Manually_Set()
    {
        using var tempStorePath = TempStorePath.Create();

        var options = new MappedFileQueueOptions { StorePath = tempStorePath.Path, SegmentSize = 32 };

        using var queue = MappedFileQueue.Create<TestStructSize16>(options);

        var producer = queue.Producer;

        long secondMessageOffset = 0;
        for (var i = 0; i < 5; i++)
        {
            if (i == 1)
            {
                // Save the offset of the second message
                secondMessageOffset = producer.Offset;
            }

            var testStruct = new TestStructSize16 { A = i, B = i + 1, C = i + 2, D = i + 3 };
            producer.Produce(ref testStruct);
        }

        var consumer = queue.Consumer;

        // Manually set the offset to consume from
        consumer.AdjustOffset(secondMessageOffset);

        for (var i = 1; i < 5; i++)
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
    public void Producer_Force_Flush_Works()
    {
        using var tempStorePath1 = TempStorePath.Create();

        var options = new MappedFileQueueOptions
        {
            StorePath = tempStorePath1.Path,
            SegmentSize = 32,
            ProducerForceFlushIntervalCount = 5
        };

        using var queue = MappedFileQueue.Create<TestStructSize16>(options);

        var producer = queue.Producer;

        var consumer = queue.Consumer;

        for (var i = 0; i < 10; i++)
        {
            var testStruct = new TestStructSize16 { A = i, B = i + 1, C = i + 2, D = i + 3 };

            producer.Produce(ref testStruct);

            if (i is 4 or 9)
            {
                // After producing 5 and 10 messages, the confirmed offset should be updated
                Assert.Equal((i + 1) * (Unsafe.SizeOf<TestStructSize16>() + Constants.EndMarkerSize),
                    producer.ConfirmedOffset);
            }
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
    public void Producer_AdjustOffset_Throws_When_Producing()
    {
        using var tempStorePath = TempStorePath.Create();

        var options = new MappedFileQueueOptions { StorePath = tempStorePath.Path, SegmentSize = 64 };

        using var queue = MappedFileQueue.Create<TestStructSize16>(options);

        var producer = queue.Producer;

        var testStruct = new TestStructSize16 { A = 1, B = 2, C = 3, D = 4 };

        producer.Produce(ref testStruct);

        Assert.Throws<InvalidOperationException>(() => producer.AdjustOffset(0));
    }

    [Fact]
    public void Consumer_AdjustOffset_Throws_When_Consuming()
    {
        using var tempStorePath = TempStorePath.Create();

        var options = new MappedFileQueueOptions { StorePath = tempStorePath.Path, SegmentSize = 32 };

        using var queue = MappedFileQueue.Create<TestStructSize16>(options);

        var producer = queue.Producer;

        for (var i = 0; i < 3; i++)
        {
            var testStruct = new TestStructSize16 { A = i, B = i + 1, C = i + 2, D = i + 3 };
            producer.Produce(ref testStruct);
        }

        var consumer = queue.Consumer;

        consumer.Consume(out _);

        Assert.Throws<InvalidOperationException>(() => consumer.AdjustOffset(0));
    }

    [Fact]
    public void Dispose_Multiple_Times_Does_Not_Throw()
    {
        using var tempStorePath = TempStorePath.Create();

        var options = new MappedFileQueueOptions { StorePath = tempStorePath.Path, SegmentSize = 64 };

        var queue = MappedFileQueue.Create<TestStructSize16>(options);

        // Dispose multiple times
        queue.Dispose();
        queue.Dispose();
    }

    [Fact]
    public void Produce_After_Dispose_Throws()
    {
        using var tempStorePath = TempStorePath.Create();
        var options = new MappedFileQueueOptions { StorePath = tempStorePath.Path, SegmentSize = 64 };
        using var queue = MappedFileQueue.Create<TestStructSize16>(options);
        var producer = queue.Producer;
        queue.Dispose();
        var testStruct = new TestStructSize16 { A = 1, B = 2, C = 3, D = 4 };
        Assert.Throws<ObjectDisposedException>(() => producer.Produce(ref testStruct));
    }

    [Fact]
    public void Consume_After_Dispose_Throws()
    {
        using var tempStorePath = TempStorePath.Create();
        var options = new MappedFileQueueOptions { StorePath = tempStorePath.Path, SegmentSize = 64 };
        using var queue = MappedFileQueue.Create<TestStructSize16>(options);
        var consumer = queue.Consumer;
        queue.Dispose();
        Assert.Throws<ObjectDisposedException>(() => consumer.Consume(out _));
    }

    [Fact]
    public void Recover_Producer_Offset_After_Crash()
    {
        using var tempStorePath = TempStorePath.Create();

        var options = new MappedFileQueueOptions
        {
            StorePath = tempStorePath.Path,
            SegmentSize = 32,
            ProducerForceFlushIntervalCount = 4
        };

        var messageSize = Unsafe.SizeOf<TestStructSize16>() + Constants.EndMarkerSize;

        // Simulate first run
        using (var queue = MappedFileQueue.Create<TestStructSize16>(options))
        {
            var producer = queue.Producer;

            for (var i = 0; i < 5; i++)
            {
                var testStruct = new TestStructSize16 { A = i, B = i + 1, C = i + 2, D = i + 3 };
                producer.Produce(ref testStruct);
            }

            Assert.Equal(4 * messageSize, producer.ConfirmedOffset);

            var consumer = queue.Consumer;

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

        // Simulate crash by modify the producer offset to be beyond the confirmed offset
        using (var queue = MappedFileQueue.Create<TestStructSize16>(options))
        {
            var producer = queue.Producer;
            var consumer = queue.Consumer;

            // Move the producer offset to the end (simulate uncommitted data)
            producer.AdjustOffset(7 * messageSize);
        }

        // Simulate recovery run
        using (var queue = MappedFileQueue.Create<TestStructSize16>(options))
        {
            var producer = queue.Producer;
            var consumer = queue.Consumer;

            // Producer's offset should be rolled back to the max(confirmed offset, consumer offset) = 5 * messageSize
            Assert.Equal(5 * messageSize, producer.Offset);

            for (var i = 4; i < 10; i++)
            {
                var testStruct = new TestStructSize16 { A = i, B = i + 1, C = i + 2, D = i + 3 };
                producer.Produce(ref testStruct);
            }

            for (var i = 4; i < 10; i++)
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
}
