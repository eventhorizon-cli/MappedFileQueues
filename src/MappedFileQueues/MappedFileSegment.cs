using System.Diagnostics.CodeAnalysis;
using System.IO.MemoryMappedFiles;
using System.Runtime.InteropServices;

namespace MappedFileQueues;

internal sealed class MappedFileSegment<T> : IDisposable where T : struct
{
    private readonly FileStream _fileStream;
    private readonly MemoryMappedFile _mmf;
    private readonly MemoryMappedViewAccessor _viewAccessor;
    private readonly int _payloadSize;

    private MappedFileSegment(
        string filePath,
        long fileSize,
        long fileStartOffset,
        bool readOnly)
    {
        if (fileSize <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(fileSize), "File size must be greater than zero.");
        }

        if (fileStartOffset < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(fileStartOffset),
                "File start offset must be greater than or equal to zero.");
        }

        StartOffset = fileStartOffset;

        _payloadSize = Marshal.SizeOf<T>();
        var messageSize = _payloadSize + Constants.EndMarkerSize;

        AllowedItemCount = fileSize / messageSize;
        AllowedLastOffsetToWrite = fileStartOffset + (AllowedItemCount - 1) * messageSize;
        EndOffset = AllowedLastOffsetToWrite + messageSize;

        var adjustedFileSize = EndOffset - fileStartOffset + 1;
        Size = adjustedFileSize;

        _fileStream = new FileStream(
            filePath,
            readOnly ? FileMode.Open : FileMode.OpenOrCreate,
            FileAccess.ReadWrite,
            FileShare.ReadWrite);

        _mmf = MemoryMappedFile.CreateFromFile(
            _fileStream,
            null,
            adjustedFileSize,
            MemoryMappedFileAccess.ReadWrite,
            HandleInheritability.None,
            true);

        _viewAccessor = _mmf.CreateViewAccessor(0, adjustedFileSize);
    }

    /// <summary>
    /// The size of the segment in bytes.
    /// </summary>
    public long Size { get; }

    /// <summary>
    /// The maximum number of items that can be stored in this segment.
    /// </summary>
    public long AllowedItemCount { get; }

    /// <summary>
    /// The start offset of the segment, which is the first valid offset for writing.
    /// </summary>
    public long StartOffset { get; }

    /// <summary>
    /// The end offset of the segment, which is the last valid offset plus the size of the item.
    /// </summary>
    public long EndOffset { get; }

    /// <summary>
    /// The last offset that can be used for writing items in this segment.
    /// </summary>
    public long AllowedLastOffsetToWrite { get; }

    public void Write(long offset, ref T message)
    {
        if (offset > AllowedLastOffsetToWrite)
        {
            throw new ArgumentOutOfRangeException(nameof(offset),
                $"Offset {offset} must be less than the allowed end offset {AllowedLastOffsetToWrite}.");
        }

        var segmentRelativeOffset = offset - StartOffset;

        if (segmentRelativeOffset < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(offset),
                $"Offset {offset} must be greater than or equal to the start offset {StartOffset}.");
        }

        _viewAccessor.Write(segmentRelativeOffset, ref message);
        _viewAccessor.Write(segmentRelativeOffset + _payloadSize, Constants.EndMarker);
    }

    public bool TryRead(long offset, out T message)
    {
        if (offset > AllowedLastOffsetToWrite)
        {
            throw new ArgumentOutOfRangeException(nameof(offset),
                $"Offset {offset} must be less than the allowed end offset {AllowedLastOffsetToWrite}.");
        }

        var segmentRelativeOffset = offset - StartOffset;

        if (segmentRelativeOffset < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(offset),
                $"Offset {offset} must be greater than or equal to the start offset {StartOffset}.");
        }

        var endMarker = _viewAccessor.ReadByte(segmentRelativeOffset + _payloadSize);

        if (endMarker != Constants.EndMarker)
        {
            message = default;
            return false;
        }

        _viewAccessor.Read(segmentRelativeOffset, out message);
        return true;
    }

    public void Dispose()
    {
        _viewAccessor.Dispose();
        _mmf.Dispose();
        _fileStream.Dispose();
    }

    /// <summary>
    /// Finds or creates a new <see cref="MappedFileSegment{T}"/> instance based on the specified parameters.
    /// </summary>
    /// <param name="directory">The directory path where the files are stored.</param>
    /// <param name="fileSize">The size of the file, may be adjusted to fit the data type.</param>
    /// <param name="offset">The offset of the item stored in the file.</param>
    /// <returns>A new instance of <see cref="MappedFileSegment{T}"/>.</returns>
    public static MappedFileSegment<T> FindOrCreate(
        string directory,
        long fileSize,
        long offset)
    {
        var fileStartOffset = GetFileStartOffset(fileSize, offset);
        var fileName = fileStartOffset.ToString("D20");

        var filePath = Path.Combine(directory, fileName);

        if (!Directory.Exists(directory))
        {
            Directory.CreateDirectory(directory);
        }

        return new MappedFileSegment<T>(
            filePath,
            fileSize,
            fileStartOffset,
            readOnly: false);
    }

    /// <summary>
    /// Tries to find a <see cref="MappedFileSegment{T}"/> instance based on the specified parameters.
    /// </summary>
    /// <param name="directory">The directory path where the files are stored.</param>
    /// <param name="fileSize">The size of the file, may be adjusted to fit the data type.</param>
    /// <param name="offset">The offset of the item stored in the file.</param>
    /// <param name="segment">The found segment, or null if not found.</param>
    /// <returns>True if the segment was found; otherwise, false.</returns>
    public static bool TryFind(
        string directory,
        long fileSize,
        long offset,
        [MaybeNullWhen(false)] out MappedFileSegment<T> segment)
    {
        var fileStartOffset = GetFileStartOffset(fileSize, offset);
        var fileName = fileStartOffset.ToString("D20");

        var filePath = Path.Combine(directory, fileName);

        if (!File.Exists(filePath))
        {
            segment = null;
            return false;
        }

        segment = new MappedFileSegment<T>(
            filePath,
            fileSize,
            fileStartOffset,
            readOnly: true);
        return true;
    }

    private static long GetFileStartOffset(long fileSize, long offset)
    {
        var payloadSize = Marshal.SizeOf<T>();
        var maxItems = fileSize / (payloadSize + 1);
        var adjustedFileSize = maxItems * (payloadSize + 1);
        var fileStartOffset = offset / adjustedFileSize * adjustedFileSize;

        return fileStartOffset;
    }
}
