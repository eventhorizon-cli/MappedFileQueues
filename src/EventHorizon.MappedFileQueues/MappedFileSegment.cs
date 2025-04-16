using System.Diagnostics.CodeAnalysis;
using System.IO.MemoryMappedFiles;
using System.Runtime.InteropServices;
using EventHorizon.MappedFileQueues;

namespace MemoryMappedFileQueue;

internal sealed class MappedFileSegment<T> : IDisposable where T : struct
{
    private readonly MemoryMappedFile _mmf;
    private readonly MemoryMappedViewAccessor _viewAccessor;

    public MappedFileSegment(
        string filePath,
        int fileSize,
        long fileStartOffset,
        FileMode fileMode,
        MemoryMappedFileAccess access)
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

        Size = fileSize;
        StartOffset = fileStartOffset;
        // 1 byte for magic byte
        var itemSize = Marshal.SizeOf<T>();
        AllowedItemCount = fileSize / (itemSize + 1);
        AllowedEndOffset = fileStartOffset + (AllowedItemCount - 1) * (itemSize + 1);

        _mmf = MemoryMappedFile.CreateFromFile(
            filePath,
            fileMode,
            null,
            fileSize,
            access);

        _viewAccessor = _mmf.CreateViewAccessor(0, fileSize);
    }

    public long Size { get; }

    public int AllowedItemCount { get; }

    public long StartOffset { get; }

    /// <summary>
    /// The maximum offset that can be used for writing.
    /// </summary>
    public long AllowedEndOffset { get; }

    public void Write(long offset, ref T value)
    {
        var actualOffset = offset - StartOffset;

        if (actualOffset < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(offset),
                $"Offset {offset} must be greater than or equal to the start offset {StartOffset}.");
        }

        if (actualOffset > AllowedEndOffset)
        {
            throw new ArgumentOutOfRangeException(nameof(offset),
                $"Offset {offset} must be less than the allowed end offset {AllowedEndOffset}.");
        }

        _viewAccessor.Write(actualOffset, Constants.MagicByte);
        _viewAccessor.Write(actualOffset + 1, ref value);
    }

    public bool TryRead(long offset, out T value)
    {
        var actualOffset = offset - StartOffset;

        if (actualOffset < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(offset),
                $"Offset {offset} must be greater than or equal to the start offset {StartOffset}.");
        }

        if (actualOffset > AllowedEndOffset)
        {
            throw new ArgumentOutOfRangeException(nameof(offset),
                $"Offset {offset} must be less than the allowed end offset {AllowedEndOffset}.");
        }

        var magicByte = _viewAccessor.ReadByte(actualOffset);

        if (magicByte != Constants.MagicByte)
        {
            value = default;
            return false;
        }

        _viewAccessor.Read(actualOffset + 1, out value);
        return true;
    }

    public void Dispose()
    {
        _viewAccessor.Dispose();
        _mmf.Dispose();
    }

    /// <summary>
    /// Try to creat or find the file segment which contains the offset.
    /// </summary>
    /// <param name="directory">The directory path where the files is stored.</param>
    /// <param name="fileSize">The size of the file.</param>
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
        var fileName = GetFileName(fileSize, offset);

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

            if (!File.Exists(filePath))
            {
                using var stream = new FileStream(
                    filePath, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.ReadWrite);
                stream.SetLength(fileSize);
            }
        }

        segment = new MappedFileSegment<T>(
            filePath,
            fileSize,
            offset,
            FileMode.Open,
            MemoryMappedFileAccess.ReadWrite);

        return true;
    }

    // get the name of the file which contains the offset
    private static string GetFileName(int fileSize, long offset)
    {
        var itemSize = Marshal.SizeOf<T>();
        var maxItems = fileSize / (itemSize + 1);
        var maxBytesCanBeUsed = maxItems * (itemSize + 1);
        var fileName = offset / maxBytesCanBeUsed * maxBytesCanBeUsed;
        return fileName.ToString("D20");
    }
}