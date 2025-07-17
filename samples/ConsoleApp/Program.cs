using System.Runtime.CompilerServices;
using ConsoleApp;
using MappedFileQueues;

var storePath = "test";

// If you have run the test before, delete the previous data first
if (Directory.Exists(storePath))
{
    Directory.Delete(storePath, true);
}

var queue = MappedFileQueue.Create<TestStruct>(new MappedFileQueueOptions
{
    StorePath = storePath,
    SegmentSize = 512 * 1024 * 1024 // 512 MB
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
