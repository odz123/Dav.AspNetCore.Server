using System.Buffers;

namespace Dav.AspNetCore.Server.Performance;

/// <summary>
/// Provides optimized file stream creation for WebDAV streaming scenarios.
/// Uses appropriate FileOptions and buffer sizes for different access patterns.
/// </summary>
public static class OptimizedFileStream
{
    /// <summary>
    /// Disk sector size for buffer alignment (typical modern drives).
    /// </summary>
    private const int SectorSize = 4096;

    /// <summary>
    /// Read-ahead buffer for sequential streaming (256KB aligned).
    /// </summary>
    private const int SequentialReadAhead = 256 * 1024;

    /// <summary>
    /// Buffer for random access / seeking (64KB aligned).
    /// </summary>
    private const int RandomAccessBuffer = 64 * 1024;

    /// <summary>
    /// Opens a file stream optimized for sequential streaming (full file downloads).
    /// Uses SequentialScan hint to optimize OS read-ahead.
    /// </summary>
    /// <param name="path">The file path.</param>
    /// <returns>An optimized FileStream for sequential reading.</returns>
    public static FileStream OpenForSequentialRead(string path)
    {
        return new FileStream(
            path,
            FileMode.Open,
            FileAccess.Read,
            FileShare.Read,
            bufferSize: SequentialReadAhead,
            options: FileOptions.Asynchronous | FileOptions.SequentialScan);
    }

    /// <summary>
    /// Opens a file stream optimized for random access (seeking/range requests).
    /// Uses RandomAccess hint for optimal seek performance.
    /// </summary>
    /// <param name="path">The file path.</param>
    /// <returns>An optimized FileStream for random access.</returns>
    public static FileStream OpenForRandomAccess(string path)
    {
        return new FileStream(
            path,
            FileMode.Open,
            FileAccess.Read,
            FileShare.Read,
            bufferSize: RandomAccessBuffer,
            options: FileOptions.Asynchronous | FileOptions.RandomAccess);
    }

    /// <summary>
    /// Opens a file stream optimized for the given access pattern.
    /// </summary>
    /// <param name="path">The file path.</param>
    /// <param name="accessPattern">The expected access pattern.</param>
    /// <returns>An optimized FileStream.</returns>
    public static FileStream OpenForRead(string path, FileAccessPattern accessPattern)
    {
        return accessPattern switch
        {
            FileAccessPattern.Sequential => OpenForSequentialRead(path),
            FileAccessPattern.RandomAccess => OpenForRandomAccess(path),
            _ => OpenForSequentialRead(path)
        };
    }

    /// <summary>
    /// Opens a file stream optimized for writing with proper buffering.
    /// </summary>
    /// <param name="path">The file path.</param>
    /// <returns>An optimized FileStream for writing.</returns>
    public static FileStream OpenForWrite(string path)
    {
        return new FileStream(
            path,
            FileMode.Create,
            FileAccess.Write,
            FileShare.None,
            bufferSize: SequentialReadAhead,
            options: FileOptions.Asynchronous | FileOptions.SequentialScan);
    }

    /// <summary>
    /// Gets an aligned buffer size for optimal I/O performance.
    /// Aligns to sector size and caps at a reasonable maximum.
    /// </summary>
    /// <param name="requestedSize">The requested buffer size.</param>
    /// <returns>An aligned buffer size.</returns>
    public static int GetAlignedBufferSize(int requestedSize)
    {
        // Round up to nearest sector size
        return ((requestedSize + SectorSize - 1) / SectorSize) * SectorSize;
    }

    /// <summary>
    /// Gets the optimal buffer size for a given content length and access pattern.
    /// </summary>
    /// <param name="contentLength">The content length.</param>
    /// <param name="accessPattern">The access pattern.</param>
    /// <returns>The optimal buffer size.</returns>
    public static int GetOptimalBufferSize(long contentLength, FileAccessPattern accessPattern)
    {
        if (accessPattern == FileAccessPattern.RandomAccess)
        {
            // For seeking, smaller buffers are more efficient
            return RandomAccessBuffer;
        }

        // For sequential access, use larger buffers
        return BufferPool.GetOptimalBufferSize(contentLength);
    }

    /// <summary>
    /// Opens a file stream asynchronously without blocking the calling thread.
    /// This is important because FileStream constructor performs synchronous I/O
    /// which can block the thread pool in high-concurrency scenarios.
    /// </summary>
    /// <param name="path">The file path.</param>
    /// <param name="accessPattern">The expected access pattern.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>An optimized FileStream.</returns>
    public static ValueTask<FileStream> OpenForReadAsync(
        string path,
        FileAccessPattern accessPattern,
        CancellationToken cancellationToken = default)
    {
        // For most cases, direct open is fast enough (file is in OS cache)
        // Only offload to thread pool for potentially slow opens
        if (IsLikelyFastOpen(path))
        {
            return new ValueTask<FileStream>(OpenForRead(path, accessPattern));
        }

        // Offload to thread pool to avoid blocking
        return new ValueTask<FileStream>(
            Task.Run(() => OpenForRead(path, accessPattern), cancellationToken));
    }

    /// <summary>
    /// Heuristic to determine if a file open is likely to be fast.
    /// Local SSDs are fast, network paths may be slow.
    /// </summary>
    private static bool IsLikelyFastOpen(string path)
    {
        // Network paths (UNC) may be slow
        if (path.StartsWith("\\\\") || path.StartsWith("//"))
            return false;

        // Windows mapped drives could be network
        if (OperatingSystem.IsWindows() && path.Length >= 2 && path[1] == ':')
        {
            // Assume local drives are fast
            return true;
        }

        // Linux/Mac: assume local paths are fast
        return true;
    }
}

/// <summary>
/// Indicates the expected file access pattern.
/// </summary>
public enum FileAccessPattern
{
    /// <summary>
    /// Sequential reading from start to end.
    /// </summary>
    Sequential,

    /// <summary>
    /// Random access / seeking within the file.
    /// </summary>
    RandomAccess
}
