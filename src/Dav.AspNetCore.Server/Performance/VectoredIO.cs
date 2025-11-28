using System.Runtime.InteropServices;

namespace Dav.AspNetCore.Server.Performance;

/// <summary>
/// Provides vectored (scatter/gather) I/O operations for efficient multi-buffer transfers.
/// Uses readv/writev on Linux for single-syscall multi-buffer operations.
/// </summary>
internal static class VectoredIO
{
    // Maximum iov entries per call (Linux default is typically 1024)
    private const int MaxIovEntries = 1024;

    // Platform detection
    private static readonly bool IsLinux = RuntimeInformation.IsOSPlatform(OSPlatform.Linux);
    private static readonly Lazy<bool> LazyIsAvailable = new(CheckVectoredIOAvailable);

    /// <summary>
    /// Indicates if vectored I/O is available.
    /// </summary>
    public static bool IsAvailable => LazyIsAvailable.Value;

    [StructLayout(LayoutKind.Sequential)]
    private struct Iovec
    {
        public IntPtr Base;
        public nuint Length;
    }

    [DllImport("libc", EntryPoint = "readv", SetLastError = true)]
    private static extern long readv(int fd, Iovec[] iov, int iovcnt);

    [DllImport("libc", EntryPoint = "writev", SetLastError = true)]
    private static extern long writev(int fd, Iovec[] iov, int iovcnt);

    [DllImport("libc", EntryPoint = "preadv", SetLastError = true)]
    private static extern long preadv(int fd, Iovec[] iov, int iovcnt, long offset);

    [DllImport("libc", EntryPoint = "pwritev", SetLastError = true)]
    private static extern long pwritev(int fd, Iovec[] iov, int iovcnt, long offset);

    private static bool CheckVectoredIOAvailable()
    {
        if (!IsLinux)
            return false;

        try
        {
            var iov = new Iovec[1];
            var result = readv(-1, iov, 0);
            // Syscall is available even if it returned error
            return true;
        }
        catch
        {
            return false;
        }
    }

    /// <summary>
    /// Reads data into multiple buffers with a single syscall.
    /// </summary>
    /// <param name="fd">File descriptor.</param>
    /// <param name="buffers">Array of buffers to read into.</param>
    /// <returns>Total bytes read, or -1 on error.</returns>
    public static long ReadV(int fd, Span<Memory<byte>> buffers)
    {
        if (!IsAvailable || fd < 0 || buffers.IsEmpty)
            return -1;

        var iovCount = Math.Min(buffers.Length, MaxIovEntries);
        var iov = new Iovec[iovCount];
        var handles = new GCHandle[iovCount];

        try
        {
            for (int i = 0; i < iovCount; i++)
            {
                var buffer = buffers[i];
                handles[i] = GCHandle.Alloc(buffer, GCHandleType.Pinned);
                iov[i].Base = handles[i].AddrOfPinnedObject();
                iov[i].Length = (nuint)buffer.Length;
            }

            return readv(fd, iov, iovCount);
        }
        finally
        {
            foreach (var handle in handles)
            {
                if (handle.IsAllocated)
                    handle.Free();
            }
        }
    }

    /// <summary>
    /// Reads data at a specific offset into multiple buffers.
    /// Does not change the file offset.
    /// </summary>
    /// <param name="fd">File descriptor.</param>
    /// <param name="buffers">Array of buffers to read into.</param>
    /// <param name="offset">File offset to read from.</param>
    /// <returns>Total bytes read, or -1 on error.</returns>
    public static long PReadV(int fd, Span<Memory<byte>> buffers, long offset)
    {
        if (!IsAvailable || fd < 0 || buffers.IsEmpty || offset < 0)
            return -1;

        var iovCount = Math.Min(buffers.Length, MaxIovEntries);
        var iov = new Iovec[iovCount];
        var handles = new GCHandle[iovCount];

        try
        {
            for (int i = 0; i < iovCount; i++)
            {
                var buffer = buffers[i];
                handles[i] = GCHandle.Alloc(buffer, GCHandleType.Pinned);
                iov[i].Base = handles[i].AddrOfPinnedObject();
                iov[i].Length = (nuint)buffer.Length;
            }

            return preadv(fd, iov, iovCount, offset);
        }
        finally
        {
            foreach (var handle in handles)
            {
                if (handle.IsAllocated)
                    handle.Free();
            }
        }
    }

    /// <summary>
    /// Writes data from multiple buffers with a single syscall.
    /// </summary>
    /// <param name="fd">File descriptor.</param>
    /// <param name="buffers">Array of buffers to write from.</param>
    /// <returns>Total bytes written, or -1 on error.</returns>
    public static long WriteV(int fd, ReadOnlySpan<ReadOnlyMemory<byte>> buffers)
    {
        if (!IsAvailable || fd < 0 || buffers.IsEmpty)
            return -1;

        var iovCount = Math.Min(buffers.Length, MaxIovEntries);
        var iov = new Iovec[iovCount];
        var handles = new GCHandle[iovCount];

        try
        {
            for (int i = 0; i < iovCount; i++)
            {
                var buffer = buffers[i];
                handles[i] = GCHandle.Alloc(buffer, GCHandleType.Pinned);
                iov[i].Base = handles[i].AddrOfPinnedObject();
                iov[i].Length = (nuint)buffer.Length;
            }

            return writev(fd, iov, iovCount);
        }
        finally
        {
            foreach (var handle in handles)
            {
                if (handle.IsAllocated)
                    handle.Free();
            }
        }
    }

    /// <summary>
    /// Reads multiple chunks from a file efficiently using preadv.
    /// Useful for prefetching non-contiguous ranges.
    /// </summary>
    /// <param name="stream">The file stream.</param>
    /// <param name="chunks">Array of (offset, length) tuples to read.</param>
    /// <returns>Array of byte arrays with read data.</returns>
    public static byte[][] ReadMultipleChunks(FileStream stream, (long Offset, int Length)[] chunks)
    {
        if (!IsAvailable || chunks.Length == 0)
        {
            return FallbackReadChunks(stream, chunks);
        }

        var fd = LinuxKernelHints.GetFileDescriptor(stream);
        if (fd < 0)
        {
            return FallbackReadChunks(stream, chunks);
        }

        var results = new byte[chunks.Length][];
        var iovCount = Math.Min(chunks.Length, MaxIovEntries);
        var iov = new Iovec[iovCount];
        var handles = new GCHandle[iovCount];

        try
        {
            // Allocate buffers
            for (int i = 0; i < iovCount; i++)
            {
                results[i] = new byte[chunks[i].Length];
                handles[i] = GCHandle.Alloc(results[i], GCHandleType.Pinned);
                iov[i].Base = handles[i].AddrOfPinnedObject();
                iov[i].Length = (nuint)chunks[i].Length;
            }

            // Use preadv for each offset (vectored but still needs multiple calls for different offsets)
            // For true scatter-gather at different offsets, we'd need io_uring
            for (int i = 0; i < iovCount; i++)
            {
                var singleIov = new Iovec[] { iov[i] };
                preadv(fd, singleIov, 1, chunks[i].Offset);
            }

            // Handle remaining chunks beyond MaxIovEntries
            for (int i = iovCount; i < chunks.Length; i++)
            {
                results[i] = new byte[chunks[i].Length];
                stream.Seek(chunks[i].Offset, SeekOrigin.Begin);
                stream.Read(results[i], 0, chunks[i].Length);
            }

            return results;
        }
        finally
        {
            foreach (var handle in handles)
            {
                if (handle.IsAllocated)
                    handle.Free();
            }
        }
    }

    private static byte[][] FallbackReadChunks(FileStream stream, (long Offset, int Length)[] chunks)
    {
        var results = new byte[chunks.Length][];

        for (int i = 0; i < chunks.Length; i++)
        {
            results[i] = new byte[chunks[i].Length];
            stream.Seek(chunks[i].Offset, SeekOrigin.Begin);
            stream.Read(results[i], 0, chunks[i].Length);
        }

        return results;
    }

    /// <summary>
    /// Reads a contiguous range into multiple buffers efficiently.
    /// Useful when you have pre-allocated buffer pools.
    /// </summary>
    /// <param name="stream">The file stream.</param>
    /// <param name="offset">Starting offset.</param>
    /// <param name="buffers">Array of buffers to fill.</param>
    /// <returns>Total bytes read.</returns>
    public static long ReadContiguousRange(FileStream stream, long offset, Memory<byte>[] buffers)
    {
        if (!IsAvailable || buffers.Length == 0)
        {
            return FallbackReadContiguous(stream, offset, buffers);
        }

        var fd = LinuxKernelHints.GetFileDescriptor(stream);
        if (fd < 0)
        {
            return FallbackReadContiguous(stream, offset, buffers);
        }

        var iovCount = Math.Min(buffers.Length, MaxIovEntries);
        var iov = new Iovec[iovCount];
        var handles = new GCHandle[iovCount];

        try
        {
            for (int i = 0; i < iovCount; i++)
            {
                handles[i] = GCHandle.Alloc(buffers[i], GCHandleType.Pinned);
                iov[i].Base = handles[i].AddrOfPinnedObject();
                iov[i].Length = (nuint)buffers[i].Length;
            }

            return preadv(fd, iov, iovCount, offset);
        }
        finally
        {
            foreach (var handle in handles)
            {
                if (handle.IsAllocated)
                    handle.Free();
            }
        }
    }

    private static long FallbackReadContiguous(FileStream stream, long offset, Memory<byte>[] buffers)
    {
        stream.Seek(offset, SeekOrigin.Begin);
        long totalRead = 0;

        foreach (var buffer in buffers)
        {
            var bytesRead = stream.Read(buffer.Span);
            totalRead += bytesRead;
            if (bytesRead < buffer.Length)
                break;
        }

        return totalRead;
    }
}

/// <summary>
/// Helper for building vectored I/O operations.
/// </summary>
internal sealed class VectoredIOBuilder
{
    private readonly List<Memory<byte>> _buffers = new();
    private long _totalLength;

    /// <summary>
    /// Adds a buffer to the vectored operation.
    /// </summary>
    public VectoredIOBuilder AddBuffer(Memory<byte> buffer)
    {
        _buffers.Add(buffer);
        _totalLength += buffer.Length;
        return this;
    }

    /// <summary>
    /// Adds a new buffer of the specified size.
    /// </summary>
    public VectoredIOBuilder AddBuffer(int size)
    {
        var buffer = new byte[size];
        _buffers.Add(buffer);
        _totalLength += size;
        return this;
    }

    /// <summary>
    /// Gets the total length of all buffers.
    /// </summary>
    public long TotalLength => _totalLength;

    /// <summary>
    /// Gets the buffers as an array.
    /// </summary>
    public Memory<byte>[] ToArray() => _buffers.ToArray();

    /// <summary>
    /// Clears all buffers.
    /// </summary>
    public void Clear()
    {
        _buffers.Clear();
        _totalLength = 0;
    }
}
