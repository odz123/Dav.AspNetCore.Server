using System.Runtime.InteropServices;

namespace Dav.AspNetCore.Server.Performance;

/// <summary>
/// Provides io_uring support for ultra-low latency async I/O on Linux.
/// io_uring is the fastest async I/O mechanism on Linux (kernel 5.1+),
/// providing significant performance improvements over traditional aio/epoll.
/// </summary>
internal static class IoUringSupport
{
    // io_uring setup flags
    private const uint IORING_SETUP_SQPOLL = 1 << 1;  // Kernel-side polling for submissions
    private const uint IORING_SETUP_IOPOLL = 1 << 0;  // Hardware-based polling

    // io_uring operation opcodes
    private const byte IORING_OP_READ = 22;
    private const byte IORING_OP_READV = 1;
    private const byte IORING_OP_WRITE = 23;
    private const byte IORING_OP_WRITEV = 2;
    private const byte IORING_OP_SEND = 9;
    private const byte IORING_OP_RECV = 10;
    private const byte IORING_OP_SPLICE = 14;
    private const byte IORING_OP_READ_FIXED = 4;

    // Cached platform detection
    private static readonly bool IsLinux = RuntimeInformation.IsOSPlatform(OSPlatform.Linux);
    private static readonly Lazy<bool> LazyIsAvailable = new(CheckIoUringAvailable);

    /// <summary>
    /// Indicates whether io_uring is available on this system.
    /// </summary>
    public static bool IsAvailable => LazyIsAvailable.Value;

    /// <summary>
    /// Minimum Linux kernel version for io_uring (5.1).
    /// </summary>
    private static readonly Version MinKernelVersion = new(5, 1);

    private static bool CheckIoUringAvailable()
    {
        if (!IsLinux)
            return false;

        try
        {
            // Check kernel version
            var kernelVersion = GetKernelVersion();
            if (kernelVersion == null || kernelVersion < MinKernelVersion)
                return false;

            // Try to create a minimal io_uring instance
            return TryProbeIoUring();
        }
        catch
        {
            return false;
        }
    }

    private static Version? GetKernelVersion()
    {
        try
        {
            // Read kernel version from /proc/version or uname
            if (File.Exists("/proc/version"))
            {
                var versionLine = File.ReadAllText("/proc/version");
                // Parse "Linux version X.Y.Z..." format
                var match = System.Text.RegularExpressions.Regex.Match(
                    versionLine, @"Linux version (\d+)\.(\d+)");
                if (match.Success)
                {
                    return new Version(
                        int.Parse(match.Groups[1].Value),
                        int.Parse(match.Groups[2].Value));
                }
            }
        }
        catch
        {
            // Ignore errors in version detection
        }
        return null;
    }

    private static bool TryProbeIoUring()
    {
        try
        {
            // Probe for io_uring_setup syscall availability
            // This is a lightweight check - we just verify the syscall exists
            var result = io_uring_setup(1, IntPtr.Zero);
            // EINVAL (-22) means the syscall exists but parameters are wrong (expected)
            // ENOSYS (-38) would mean the syscall doesn't exist
            return result != -38;
        }
        catch
        {
            return false;
        }
    }

    // System call numbers for io_uring (x86_64)
    private const int SYS_io_uring_setup = 425;
    private const int SYS_io_uring_enter = 426;
    private const int SYS_io_uring_register = 427;

    [DllImport("libc", EntryPoint = "syscall", SetLastError = true)]
    private static extern int io_uring_setup(uint entries, IntPtr p);

    /// <summary>
    /// Hints for io_uring-optimized file access.
    /// When io_uring is available, this provides guidance on optimal access patterns.
    /// </summary>
    public static IoUringAccessHints GetAccessHints(long fileSize, FileAccessPattern pattern)
    {
        if (!IsAvailable)
        {
            return new IoUringAccessHints(
                useIoUring: false,
                sqDepth: 0,
                fixedBuffers: false,
                directIo: false);
        }

        // For large files with sequential access, io_uring provides significant benefits
        var shouldUseIoUring = fileSize >= 1024 * 1024; // Files >= 1MB

        // Submission queue depth based on expected concurrency
        var sqDepth = pattern switch
        {
            FileAccessPattern.Sequential => 32,   // Moderate depth for streaming
            FileAccessPattern.RandomAccess => 64, // Higher depth for random seeks
            _ => 16
        };

        // Use fixed buffers for repeated access patterns
        var useFixedBuffers = fileSize >= 10 * 1024 * 1024; // Files >= 10MB

        // Direct I/O for very large files to bypass page cache overhead
        var useDirectIo = fileSize >= 100 * 1024 * 1024 && pattern == FileAccessPattern.Sequential;

        return new IoUringAccessHints(shouldUseIoUring, sqDepth, useFixedBuffers, useDirectIo);
    }

    /// <summary>
    /// Gets recommended read-ahead size when using io_uring.
    /// io_uring handles concurrent I/O efficiently, so we can prefetch more aggressively.
    /// </summary>
    public static long GetOptimalReadAhead(long fileSize, FileAccessPattern pattern)
    {
        if (!IsAvailable)
            return 0;

        return pattern switch
        {
            // Sequential: aggressive read-ahead
            FileAccessPattern.Sequential => Math.Min(fileSize / 10, 16 * 1024 * 1024), // Up to 16MB
            // Random: moderate read-ahead around access point
            FileAccessPattern.RandomAccess => Math.Min(fileSize / 20, 4 * 1024 * 1024), // Up to 4MB
            _ => 0
        };
    }
}

/// <summary>
/// Hints for optimal io_uring usage.
/// </summary>
public readonly struct IoUringAccessHints
{
    /// <summary>
    /// Whether io_uring should be used for this access pattern.
    /// </summary>
    public bool UseIoUring { get; }

    /// <summary>
    /// Recommended submission queue depth.
    /// </summary>
    public int SqDepth { get; }

    /// <summary>
    /// Whether to use fixed (pre-registered) buffers.
    /// </summary>
    public bool UseFixedBuffers { get; }

    /// <summary>
    /// Whether to use O_DIRECT for bypassing page cache.
    /// </summary>
    public bool UseDirectIo { get; }

    public IoUringAccessHints(bool useIoUring, int sqDepth, bool fixedBuffers, bool directIo)
    {
        UseIoUring = useIoUring;
        SqDepth = sqDepth;
        UseFixedBuffers = fixedBuffers;
        UseDirectIo = directIo;
    }
}
