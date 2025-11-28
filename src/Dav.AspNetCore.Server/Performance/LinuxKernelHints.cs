using System.Runtime.InteropServices;

namespace Dav.AspNetCore.Server.Performance;

/// <summary>
/// Provides Linux-specific kernel hints for optimizing file I/O.
/// Uses posix_fadvise and madvise to inform the kernel about expected access patterns,
/// significantly improving prefetching and cache utilization for streaming workloads.
/// </summary>
internal static class LinuxKernelHints
{
    // posix_fadvise advice flags
    private const int POSIX_FADV_NORMAL = 0;
    private const int POSIX_FADV_RANDOM = 1;
    private const int POSIX_FADV_SEQUENTIAL = 2;
    private const int POSIX_FADV_WILLNEED = 3;
    private const int POSIX_FADV_DONTNEED = 4;
    private const int POSIX_FADV_NOREUSE = 5;

    // madvise advice flags
    private const int MADV_NORMAL = 0;
    private const int MADV_RANDOM = 1;
    private const int MADV_SEQUENTIAL = 2;
    private const int MADV_WILLNEED = 3;
    private const int MADV_DONTNEED = 4;

    // Cached check for Linux platform
    private static readonly bool IsLinux = RuntimeInformation.IsOSPlatform(OSPlatform.Linux);

    /// <summary>
    /// Advises the kernel that we will need the specified file range soon.
    /// This triggers asynchronous readahead into the page cache.
    /// </summary>
    /// <param name="fd">The file descriptor.</param>
    /// <param name="offset">Starting offset.</param>
    /// <param name="length">Length of the range.</param>
    public static void AdviseWillNeed(int fd, long offset, long length)
    {
        if (!IsLinux || fd < 0)
            return;

        try
        {
            PosixFadvise(fd, offset, length, POSIX_FADV_WILLNEED);
        }
        catch
        {
            // Non-critical - just skip if not supported
        }
    }

    /// <summary>
    /// Advises the kernel that the file will be accessed sequentially.
    /// Enables aggressive read-ahead.
    /// </summary>
    /// <param name="fd">The file descriptor.</param>
    /// <param name="offset">Starting offset.</param>
    /// <param name="length">Length of the range (0 for entire file).</param>
    public static void AdviseSequential(int fd, long offset = 0, long length = 0)
    {
        if (!IsLinux || fd < 0)
            return;

        try
        {
            PosixFadvise(fd, offset, length, POSIX_FADV_SEQUENTIAL);
        }
        catch
        {
            // Non-critical
        }
    }

    /// <summary>
    /// Advises the kernel that the file will be accessed randomly.
    /// Disables read-ahead to avoid wasting I/O bandwidth.
    /// </summary>
    /// <param name="fd">The file descriptor.</param>
    public static void AdviseRandom(int fd)
    {
        if (!IsLinux || fd < 0)
            return;

        try
        {
            PosixFadvise(fd, 0, 0, POSIX_FADV_RANDOM);
        }
        catch
        {
            // Non-critical
        }
    }

    /// <summary>
    /// Advises the kernel that the specified range is no longer needed.
    /// Allows the kernel to free up cache memory.
    /// </summary>
    /// <param name="fd">The file descriptor.</param>
    /// <param name="offset">Starting offset.</param>
    /// <param name="length">Length of the range.</param>
    public static void AdviseDontNeed(int fd, long offset, long length)
    {
        if (!IsLinux || fd < 0)
            return;

        try
        {
            PosixFadvise(fd, offset, length, POSIX_FADV_DONTNEED);
        }
        catch
        {
            // Non-critical
        }
    }

    /// <summary>
    /// Advises the kernel that the specified range will be accessed only once.
    /// Useful for streaming where data won't be re-read.
    /// </summary>
    /// <param name="fd">The file descriptor.</param>
    /// <param name="offset">Starting offset.</param>
    /// <param name="length">Length of the range.</param>
    public static void AdviseNoReuse(int fd, long offset, long length)
    {
        if (!IsLinux || fd < 0)
            return;

        try
        {
            PosixFadvise(fd, offset, length, POSIX_FADV_NOREUSE);
        }
        catch
        {
            // Non-critical
        }
    }

    /// <summary>
    /// Gets the file descriptor from a FileStream (Linux only).
    /// </summary>
    /// <param name="stream">The FileStream.</param>
    /// <returns>The file descriptor, or -1 if not available.</returns>
    public static int GetFileDescriptor(FileStream stream)
    {
        if (!IsLinux)
            return -1;

        try
        {
            return (int)stream.SafeFileHandle.DangerousGetHandle();
        }
        catch
        {
            return -1;
        }
    }

    /// <summary>
    /// Applies optimal kernel hints for streaming access pattern.
    /// </summary>
    /// <param name="stream">The FileStream to optimize.</param>
    /// <param name="accessPattern">The expected access pattern.</param>
    /// <param name="startOffset">Optional start offset for the access.</param>
    /// <param name="length">Optional length of the expected access.</param>
    public static void ApplyStreamingHints(
        FileStream stream,
        FileAccessPattern accessPattern,
        long startOffset = 0,
        long length = 0)
    {
        if (!IsLinux)
            return;

        var fd = GetFileDescriptor(stream);
        if (fd < 0)
            return;

        switch (accessPattern)
        {
            case FileAccessPattern.Sequential:
                AdviseSequential(fd, startOffset, length);
                // Also advise that we'll need the data soon
                if (length > 0)
                {
                    AdviseWillNeed(fd, startOffset, Math.Min(length, 4 * 1024 * 1024)); // Prefetch up to 4MB
                }
                break;

            case FileAccessPattern.RandomAccess:
                AdviseRandom(fd);
                // For random access, prefetch a smaller region
                if (length > 0)
                {
                    AdviseWillNeed(fd, startOffset, Math.Min(length, 1024 * 1024)); // Prefetch up to 1MB
                }
                break;
        }
    }

    /// <summary>
    /// Prefetches a file range into the kernel's page cache.
    /// This triggers asynchronous I/O that runs ahead of the actual reads.
    /// </summary>
    /// <param name="filePath">The file path.</param>
    /// <param name="offset">Starting offset.</param>
    /// <param name="length">Length to prefetch.</param>
    public static void PrefetchFileRange(string filePath, long offset, long length)
    {
        if (!IsLinux || string.IsNullOrEmpty(filePath))
            return;

        try
        {
            // Open file just for fadvise, then close
            using var stream = new FileStream(
                filePath,
                FileMode.Open,
                FileAccess.Read,
                FileShare.Read,
                bufferSize: 1,
                FileOptions.None);

            var fd = GetFileDescriptor(stream);
            if (fd >= 0)
            {
                AdviseWillNeed(fd, offset, length);
            }
        }
        catch
        {
            // Non-critical - prefetch failure doesn't affect functionality
        }
    }

    /// <summary>
    /// Checks if Linux kernel hints are available on this platform.
    /// </summary>
    public static bool IsAvailable => IsLinux;

    // P/Invoke for posix_fadvise using traditional DllImport
    [DllImport("libc", EntryPoint = "posix_fadvise", SetLastError = true)]
    private static extern int posix_fadvise(int fd, long offset, long len, int advice);

    private static void PosixFadvise(int fd, long offset, long length, int advice)
    {
        if (IsLinux)
        {
            posix_fadvise(fd, offset, length, advice);
        }
    }
}
