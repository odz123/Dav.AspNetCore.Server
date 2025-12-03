using System.Buffers;
using System.Collections.Concurrent;
using System.Text;

namespace Dav.AspNetCore.Server.Performance;

/// <summary>
/// Specifies the expected file access pattern for stream optimization.
/// </summary>
public enum FileAccessPattern
{
    /// <summary>
    /// Sequential access - reading the file from start to end.
    /// Enables OS read-ahead optimization (FileOptions.SequentialScan).
    /// </summary>
    Sequential,

    /// <summary>
    /// Random access - seeking to various positions in the file.
    /// Disables read-ahead for better seek performance (FileOptions.RandomAccess).
    /// </summary>
    RandomAccess
}

/// <summary>
/// Provides pooled buffer operations for high-performance I/O.
/// Optimized for WebDAV streaming scenarios.
/// </summary>
internal static class BufferPool
{
    /// <summary>
    /// Default buffer size for stream operations (64KB).
    /// </summary>
    public const int DefaultBufferSize = 64 * 1024;

    /// <summary>
    /// Large buffer size for bulk operations (256KB).
    /// </summary>
    public const int LargeBufferSize = 256 * 1024;

    /// <summary>
    /// Streaming buffer size for large file transfers (1MB).
    /// Optimized for high-throughput streaming scenarios.
    /// </summary>
    public const int StreamingBufferSize = 1024 * 1024;

    /// <summary>
    /// Threshold above which streaming buffer size is used (50MB).
    /// </summary>
    public const long StreamingThreshold = 50 * 1024 * 1024;

    /// <summary>
    /// Gets the optimal buffer size for a given content length.
    /// Uses larger buffers for streaming large files.
    /// </summary>
    /// <param name="contentLength">The content length, or -1 if unknown.</param>
    /// <returns>The recommended buffer size.</returns>
    public static int GetOptimalBufferSize(long contentLength)
    {
        if (contentLength < 0)
            return DefaultBufferSize;

        if (contentLength >= StreamingThreshold)
            return StreamingBufferSize;

        if (contentLength >= 1024 * 1024) // 1MB
            return LargeBufferSize;

        return DefaultBufferSize;
    }

    /// <summary>
    /// Rents a buffer from the shared array pool.
    /// </summary>
    /// <param name="minimumLength">The minimum required length.</param>
    /// <returns>A rented buffer that must be returned.</returns>
    public static byte[] Rent(int minimumLength = DefaultBufferSize)
        => ArrayPool<byte>.Shared.Rent(minimumLength);

    /// <summary>
    /// Returns a buffer to the shared array pool.
    /// </summary>
    /// <param name="buffer">The buffer to return.</param>
    /// <param name="clearArray">Whether to clear the array before returning.</param>
    public static void Return(byte[] buffer, bool clearArray = false)
        => ArrayPool<byte>.Shared.Return(buffer, clearArray);

    /// <summary>
    /// Copies data from source stream to destination stream using pooled buffers.
    /// </summary>
    public static async Task CopyToPooledAsync(
        this Stream source,
        Stream destination,
        int bufferSize = DefaultBufferSize,
        CancellationToken cancellationToken = default)
    {
        var buffer = Rent(bufferSize);
        try
        {
            int bytesRead;
            while ((bytesRead = await source.ReadAsync(buffer.AsMemory(0, bufferSize), cancellationToken).ConfigureAwait(false)) > 0)
            {
                await destination.WriteAsync(buffer.AsMemory(0, bytesRead), cancellationToken).ConfigureAwait(false);
            }
        }
        finally
        {
            Return(buffer);
        }
    }

    /// <summary>
    /// Copies a specific number of bytes from source stream to destination stream using pooled buffers.
    /// </summary>
    public static async Task CopyToPooledAsync(
        this Stream source,
        Stream destination,
        long bytesToCopy,
        int bufferSize = DefaultBufferSize,
        CancellationToken cancellationToken = default)
    {
        var buffer = Rent(bufferSize);
        try
        {
            while (bytesToCopy > 0)
            {
                var readSize = (int)Math.Min(bytesToCopy, bufferSize);
                var bytesRead = await source.ReadAsync(buffer.AsMemory(0, readSize), cancellationToken).ConfigureAwait(false);
                if (bytesRead == 0)
                    break;

                await destination.WriteAsync(buffer.AsMemory(0, bytesRead), cancellationToken).ConfigureAwait(false);
                bytesToCopy -= bytesRead;
            }
        }
        finally
        {
            Return(buffer);
        }
    }

    /// <summary>
    /// Copies data from source to destination with pipelined reads for better throughput.
    /// Uses double-buffering to overlap read and write operations.
    /// </summary>
    public static async Task CopyToPooledPipelinedAsync(
        this Stream source,
        Stream destination,
        int bufferSize = DefaultBufferSize,
        CancellationToken cancellationToken = default)
    {
        // Double-buffer for pipelining
        var buffer1 = Rent(bufferSize);
        var buffer2 = Rent(bufferSize);
        try
        {
            // Start first read
            var currentBuffer = buffer1;
            var nextBuffer = buffer2;
            var readTask = source.ReadAsync(currentBuffer.AsMemory(0, bufferSize), cancellationToken);

            while (true)
            {
                var bytesRead = await readTask.ConfigureAwait(false);
                if (bytesRead == 0)
                    break;

                // Start next read while we write current buffer
                readTask = source.ReadAsync(nextBuffer.AsMemory(0, bufferSize), cancellationToken);

                // Write current buffer
                await destination.WriteAsync(currentBuffer.AsMemory(0, bytesRead), cancellationToken).ConfigureAwait(false);

                // Swap buffers
                (currentBuffer, nextBuffer) = (nextBuffer, currentBuffer);
            }
        }
        finally
        {
            Return(buffer1);
            Return(buffer2);
        }
    }

    /// <summary>
    /// Copies a specific number of bytes with pipelined reads for better throughput.
    /// </summary>
    public static async Task CopyToPooledPipelinedAsync(
        this Stream source,
        Stream destination,
        long bytesToCopy,
        int bufferSize = DefaultBufferSize,
        CancellationToken cancellationToken = default)
    {
        if (bytesToCopy <= bufferSize)
        {
            // For small copies, use simple method
            await source.CopyToPooledAsync(destination, bytesToCopy, bufferSize, cancellationToken).ConfigureAwait(false);
            return;
        }

        var buffer1 = Rent(bufferSize);
        var buffer2 = Rent(bufferSize);
        try
        {
            var currentBuffer = buffer1;
            var nextBuffer = buffer2;
            var remaining = bytesToCopy;

            var readSize = (int)Math.Min(remaining, bufferSize);
            var readTask = source.ReadAsync(currentBuffer.AsMemory(0, readSize), cancellationToken);

            while (remaining > 0)
            {
                var bytesRead = await readTask.ConfigureAwait(false);
                if (bytesRead == 0)
                    break;

                remaining -= bytesRead;

                // Start next read if there's more data
                if (remaining > 0)
                {
                    var nextReadSize = (int)Math.Min(remaining, bufferSize);
                    readTask = source.ReadAsync(nextBuffer.AsMemory(0, nextReadSize), cancellationToken);
                }

                // Write current buffer
                await destination.WriteAsync(currentBuffer.AsMemory(0, bytesRead), cancellationToken).ConfigureAwait(false);

                // Swap buffers
                (currentBuffer, nextBuffer) = (nextBuffer, currentBuffer);
            }
        }
        finally
        {
            Return(buffer1);
            Return(buffer2);
        }
    }
}

/// <summary>
/// Provides pooled StringBuilder instances to reduce allocations.
/// </summary>
internal static class StringBuilderPool
{
    private static readonly ConcurrentBag<StringBuilder> Pool = new();
    private const int MaxPoolSize = 32;
    private const int DefaultCapacity = 256;

    /// <summary>
    /// Rents a StringBuilder from the pool.
    /// </summary>
    public static StringBuilder Rent()
    {
        if (Pool.TryTake(out var sb))
        {
            return sb;
        }
        return new StringBuilder(DefaultCapacity);
    }

    /// <summary>
    /// Returns a StringBuilder to the pool.
    /// </summary>
    public static void Return(StringBuilder sb)
    {
        if (sb.Capacity <= 8192 && Pool.Count < MaxPoolSize)
        {
            sb.Clear();
            Pool.Add(sb);
        }
    }
}
