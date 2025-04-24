using System.Diagnostics.CodeAnalysis;
using System.IO.MemoryMappedFiles;
using System.Runtime.InteropServices;

namespace EventHorizon.MappedFileQueues;

internal sealed class MappedFileSegment<T> : IDisposable where T : struct
{
    private readonly FileStream _fileStream;
    private readonly MemoryMappedFile _mmf;
    private readonly MemoryMappedViewAccessor _viewAccessor;
    private readonly int _itemSize;

    private MappedFileSegment(
        string filePath,
        int fileSize,
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

        // 1 byte for magic byte
        _itemSize = Marshal.SizeOf<T>();
        AllowedItemCount = fileSize / (_itemSize + 1);
        AllowedLastOffsetToWrite = fileStartOffset + (AllowedItemCount - 1) * (_itemSize + 1);
        EndOffset = AllowedLastOffsetToWrite + _itemSize;

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

    public long Size { get; }

    public int AllowedItemCount { get; }

    public long StartOffset { get; }

    public long EndOffset { get; }

    /// <summary>
    /// The maximum offset that can be used for writing.
    /// </summary>
    public long AllowedLastOffsetToWrite { get; }

    public void Write(long offset, ref T value)
    {
        var actualOffset = offset - StartOffset;

        if (actualOffset < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(offset),
                $"Offset {offset} must be greater than or equal to the start offset {StartOffset}.");
        }

        if (actualOffset > AllowedLastOffsetToWrite)
        {
            throw new ArgumentOutOfRangeException(nameof(offset),
                $"Offset {offset} must be less than the allowed end offset {AllowedLastOffsetToWrite}.");
        }

        _viewAccessor.Write(actualOffset, ref value);
        _viewAccessor.Write(actualOffset + _itemSize, Constants.MagicByte);
    }

    public bool TryRead(long offset, out T value)
    {
        var actualOffset = offset - StartOffset;

        if (actualOffset < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(offset),
                $"Offset {offset} must be greater than or equal to the start offset {StartOffset}.");
        }

        if (actualOffset > AllowedLastOffsetToWrite)
        {
            throw new ArgumentOutOfRangeException(nameof(offset),
                $"Offset {offset} must be less than the allowed end offset {AllowedLastOffsetToWrite}.");
        }

        var magicByte = _viewAccessor.ReadByte(actualOffset + _itemSize);

        if (magicByte != Constants.MagicByte)
        {
            value = default;
            return false;
        }

        _viewAccessor.Read(actualOffset, out value);
        return true;
    }

    public void Dispose()
    {
        _viewAccessor.Dispose();
        _mmf.Dispose();
        _fileStream.Dispose();
    }

    /// <summary>
    /// Try to creat or find the file segment which contains the offset.
    /// </summary>
    /// <param name="directory">The directory path where the files is stored.</param>
    /// <param name="fileSize">The size of the file, may be adjusted to fit the data type.</param>
    /// <param name="offset">The offset which is stored in the file.</param>
    /// <param name="readOnly">True if the file is opened in read only mode, otherwise false.</param>
    /// <param name="segment">The segment which contains the offset.</param>
    /// <returns>True if the file exists and the segment is created, otherwise false.</returns>
    public static bool TryCreateOrFindByOffset(
        string directory,
        int fileSize,
        long offset,
        bool readOnly,
        [MaybeNullWhen(false)] out MappedFileSegment<T> segment)
    {
        var fileStartOffset = GetFileStartOffset(fileSize, offset);
        var fileName = fileStartOffset.ToString("D20");

        var filePath = Path.Combine(directory, fileName);

        if (readOnly)
        {
            if (!File.Exists(filePath))
            {
                segment = null;
                return false;
            }
        }
        else
        {
            if (!Directory.Exists(directory))
            {
                Directory.CreateDirectory(directory);
            }
        }

        segment = new MappedFileSegment<T>(
            filePath,
            fileSize,
            fileStartOffset,
            readOnly);

        return true;
    }

    private static long GetFileStartOffset(int fileSize, long offset)
    {
        var itemSize = Marshal.SizeOf<T>();
        var maxItems = fileSize / (itemSize + 1);
        var adjustedFileSize = maxItems * (itemSize + 1);
        var fileStartOffset = offset / adjustedFileSize * adjustedFileSize;

        return fileStartOffset;
    }
}