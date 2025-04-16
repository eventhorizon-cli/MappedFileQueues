using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using ConsoleApp;
using EventHorizon.MappedFileQueues;

var segmentSize = 512 * 1024 * 1024;

var itemSize = Marshal.SizeOf<TestStruct>();
var maxItems = segmentSize * 2 / itemSize;

var mappedFileQueue = MappedFileQueue.Create<TestStruct>(new MappedFileQueueOptions
{
    StorePath = "test",
    // 1GB
    SegmentSize = segmentSize
});

using var producer = mappedFileQueue.CreateProducer();

using var consumer = mappedFileQueue.CreateConsumer();

var sw = Stopwatch.StartNew();
unsafe
{
    for (var i = 1; i <= maxItems; i++)
    {
        var testStruct = new TestStruct
        {
            IntValue = i,
            LongValue = i * 10,
            DoubleValue = i / 2.0,
        };

        var testString = "TestString" + i;
        fixed (char* fixedChar = testString)
        {
            Unsafe.CopyBlock(testStruct.StringValue, fixedChar, sizeof(char) * (uint)testString.Length);
        }

        if (i == 1)
        {
            Console.WriteLine($"The first item: {nameof(testStruct.IntValue)} = {testStruct.IntValue}, " +
                              $"{nameof(testStruct.LongValue)} = {testStruct.LongValue}, " +
                              $"{nameof(testStruct.DoubleValue)} = {testStruct.DoubleValue}, " +
                              $"{nameof(testStruct.StringValue)} = {testString}");
        }

        if (i == maxItems)
        {
            Console.WriteLine($"The last item: {nameof(testStruct.IntValue)} = {testStruct.IntValue}, " +
                              $"{nameof(testStruct.LongValue)} = {testStruct.LongValue}, " +
                              $"{nameof(testStruct.DoubleValue)} = {testStruct.DoubleValue}, " +
                              $"{nameof(testStruct.StringValue)} = {testString}");
        }

        producer.Produce(ref testStruct);
    }
}

Console.WriteLine($"Completed writing {segmentSize * 2 / itemSize} items in {sw.ElapsedMilliseconds} ms");

sw.Restart();
for (var i = 1; i <= maxItems; i++)
{
    consumer.Consume(out TestStruct testStruct);
    consumer.Commit();

    if (i == 1)
    {
        unsafe
        {
            Console.WriteLine($"The first item: {nameof(testStruct.IntValue)} = {testStruct.IntValue}, " +
                              $"{nameof(testStruct.LongValue)} = {testStruct.LongValue}, " +
                              $"{nameof(testStruct.DoubleValue)} = {testStruct.DoubleValue}, " +
                              $"{nameof(testStruct.StringValue)} = {ToManagedString(testStruct.StringValue, 20)}");
        }
    }

    if (i == maxItems)
    {
        unsafe
        {
            Console.WriteLine($"The last item: {nameof(testStruct.IntValue)} = {testStruct.IntValue}, " +
                              $"{nameof(testStruct.LongValue)} = {testStruct.LongValue}, " +
                              $"{nameof(testStruct.DoubleValue)} = {testStruct.DoubleValue}, " +
                              $"{nameof(testStruct.StringValue)} = {ToManagedString(testStruct.StringValue, 20)}");
        }
    }
}

Console.WriteLine($"Completed reading {segmentSize * 2 / itemSize} items in {sw.ElapsedMilliseconds} ms");


// If you want to use the string in the struct, you can use the following method to convert it back to a managed string
unsafe string ToManagedString(char* source, int maxLength)
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