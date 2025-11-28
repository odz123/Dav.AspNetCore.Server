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
    private const int MADV_FREE = 8;
    private const int MADV_HUGEPAGE = 14;
    private const int MADV_NOHUGEPAGE = 15;
    private const int MADV_COLD = 20;
    private const int MADV_PAGEOUT = 21;
    private const int MADV_POPULATE_READ = 22;  // Linux 5.14+ - pre-fault pages for reading

    // readahead syscall for aggressive prefetching
    private const int SYS_readahead = 187; // x86_64

    // Cached check for Linux platform
    private static readonly bool IsLinux = RuntimeInformation.IsOSPlatform(OSPlatform.Linux);

    // Cached kernel version check for advanced features
    private static readonly Lazy<bool> HasAdvancedMadvise = new(CheckAdvancedMadvise);

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

    // P/Invoke for madvise (memory-mapped file hints)
    [DllImport("libc", EntryPoint = "madvise", SetLastError = true)]
    private static extern int madvise(IntPtr addr, nuint length, int advice);

    // P/Invoke for readahead (aggressive prefetching)
    [DllImport("libc", EntryPoint = "readahead", SetLastError = true)]
    private static extern int readahead(int fd, long offset, nuint count);

    private static void PosixFadvise(int fd, long offset, long length, int advice)
    {
        if (IsLinux)
        {
            posix_fadvise(fd, offset, length, advice);
        }
    }

    private static bool CheckAdvancedMadvise()
    {
        if (!IsLinux)
            return false;

        try
        {
            // Check if we're on Linux 5.14+ for MADV_POPULATE_READ
            if (File.Exists("/proc/version"))
            {
                var versionLine = File.ReadAllText("/proc/version");
                var match = System.Text.RegularExpressions.Regex.Match(
                    versionLine, @"Linux version (\d+)\.(\d+)");
                if (match.Success)
                {
                    var major = int.Parse(match.Groups[1].Value);
                    var minor = int.Parse(match.Groups[2].Value);
                    return major > 5 || (major == 5 && minor >= 14);
                }
            }
        }
        catch
        {
            // Ignore
        }
        return false;
    }

    /// <summary>
    /// Applies madvise hint for a memory-mapped region.
    /// Use for memory-mapped files to optimize kernel page management.
    /// </summary>
    /// <param name="address">The memory address.</param>
    /// <param name="length">Length of the region.</param>
    /// <param name="pattern">Expected access pattern.</param>
    public static void ApplyMadvise(IntPtr address, long length, FileAccessPattern pattern)
    {
        if (!IsLinux || address == IntPtr.Zero || length <= 0)
            return;

        try
        {
            var advice = pattern switch
            {
                FileAccessPattern.Sequential => MADV_SEQUENTIAL,
                FileAccessPattern.RandomAccess => MADV_RANDOM,
                _ => MADV_NORMAL
            };

            madvise(address, (nuint)length, advice);
        }
        catch
        {
            // Non-critical
        }
    }

    /// <summary>
    /// Pre-faults pages into memory for a memory-mapped region.
    /// Use MADV_WILLNEED to bring pages into cache before they're accessed.
    /// </summary>
    /// <param name="address">The memory address.</param>
    /// <param name="length">Length of the region to prefetch.</param>
    public static void MadviseWillNeed(IntPtr address, long length)
    {
        if (!IsLinux || address == IntPtr.Zero || length <= 0)
            return;

        try
        {
            madvise(address, (nuint)length, MADV_WILLNEED);
        }
        catch
        {
            // Non-critical
        }
    }

    /// <summary>
    /// Requests huge pages for a memory-mapped region.
    /// Huge pages reduce TLB pressure for large files, improving performance.
    /// </summary>
    /// <param name="address">The memory address.</param>
    /// <param name="length">Length of the region.</param>
    public static void MadviseHugePage(IntPtr address, long length)
    {
        if (!IsLinux || address == IntPtr.Zero || length <= 0)
            return;

        try
        {
            madvise(address, (nuint)length, MADV_HUGEPAGE);
        }
        catch
        {
            // May fail if huge pages aren't available - non-critical
        }
    }

    /// <summary>
    /// Pre-faults pages using MADV_POPULATE_READ (Linux 5.14+).
    /// This is more aggressive than MADV_WILLNEED and guarantees pages are populated.
    /// </summary>
    /// <param name="address">The memory address.</param>
    /// <param name="length">Length of the region.</param>
    /// <returns>True if successful, false if not supported or failed.</returns>
    public static bool MadvisePopulateRead(IntPtr address, long length)
    {
        if (!IsLinux || !HasAdvancedMadvise.Value || address == IntPtr.Zero || length <= 0)
            return false;

        try
        {
            var result = madvise(address, (nuint)length, MADV_POPULATE_READ);
            return result == 0;
        }
        catch
        {
            return false;
        }
    }

    /// <summary>
    /// Performs aggressive readahead using the readahead() syscall.
    /// This is more aggressive than posix_fadvise WILLNEED.
    /// </summary>
    /// <param name="fd">File descriptor.</param>
    /// <param name="offset">Starting offset.</param>
    /// <param name="length">Length to read ahead.</param>
    public static void AggressiveReadahead(int fd, long offset, long length)
    {
        if (!IsLinux || fd < 0 || length <= 0)
            return;

        try
        {
            // Cap at 32MB per call to avoid overwhelming the I/O scheduler
            const long maxReadahead = 32 * 1024 * 1024;
            var remaining = length;
            var currentOffset = offset;

            while (remaining > 0)
            {
                var chunkSize = Math.Min(remaining, maxReadahead);
                readahead(fd, currentOffset, (nuint)chunkSize);
                currentOffset += chunkSize;
                remaining -= chunkSize;
            }
        }
        catch
        {
            // Non-critical
        }
    }

    /// <summary>
    /// Applies optimal hints for NZB/video streaming.
    /// Combines multiple techniques for maximum prefetching efficiency.
    /// </summary>
    /// <param name="stream">The file stream.</param>
    /// <param name="offset">Starting offset.</param>
    /// <param name="requestedLength">Requested read length.</param>
    /// <param name="fileSize">Total file size.</param>
    public static void ApplyStreamingHintsAggressive(
        FileStream stream,
        long offset,
        long requestedLength,
        long fileSize)
    {
        if (!IsLinux)
            return;

        var fd = GetFileDescriptor(stream);
        if (fd < 0)
            return;

        try
        {
            // For initial reads (first 2MB), be extra aggressive
            if (offset < 2 * 1024 * 1024)
            {
                // Prefetch first 8MB for fast initial response
                var prefetchLength = Math.Min(8 * 1024 * 1024, fileSize);
                AdviseSequential(fd, 0, prefetchLength);
                AggressiveReadahead(fd, 0, prefetchLength);
            }
            else
            {
                // Sequential hint for current region
                AdviseSequential(fd, offset, requestedLength);

                // Aggressive read-ahead for next chunks
                var readAheadStart = offset + requestedLength;
                var readAheadLength = Math.Min(4 * 1024 * 1024, fileSize - readAheadStart);
                if (readAheadLength > 0)
                {
                    AggressiveReadahead(fd, readAheadStart, readAheadLength);
                }
            }
        }
        catch
        {
            // Non-critical
        }
    }

    /// <summary>
    /// Marks a range as no longer needed to free cache space.
    /// Useful after streaming a large segment that won't be re-read.
    /// </summary>
    /// <param name="fd">File descriptor.</param>
    /// <param name="offset">Starting offset.</param>
    /// <param name="length">Length of the range.</param>
    public static void ReleaseStreamedRange(int fd, long offset, long length)
    {
        if (!IsLinux || fd < 0)
            return;

        try
        {
            // Tell the kernel this range is no longer needed
            PosixFadvise(fd, offset, length, POSIX_FADV_DONTNEED);
        }
        catch
        {
            // Non-critical
        }
    }
}
