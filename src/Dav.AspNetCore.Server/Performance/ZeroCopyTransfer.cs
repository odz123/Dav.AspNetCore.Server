using System.Runtime.InteropServices;
using System.Net.Sockets;

namespace Dav.AspNetCore.Server.Performance;

/// <summary>
/// Provides zero-copy file transfer capabilities using OS-level optimizations.
/// Uses sendfile() on Linux for direct kernel-to-kernel data transfer,
/// and splice() for pipe-based zero-copy when sendfile isn't suitable.
/// </summary>
internal static class ZeroCopyTransfer
{
    // splice() flags
    private const int SPLICE_F_MOVE = 1;      // Move pages instead of copying
    private const int SPLICE_F_NONBLOCK = 2;  // Non-blocking operation
    private const int SPLICE_F_MORE = 4;      // More data coming

    // Platform detection
    private static readonly bool IsLinux = RuntimeInformation.IsOSPlatform(OSPlatform.Linux);
    private static readonly Lazy<bool> LazySendfileAvailable = new(CheckSendfileAvailable);
    private static readonly Lazy<bool> LazySpliceAvailable = new(CheckSpliceAvailable);

    /// <summary>
    /// Indicates if sendfile is available.
    /// </summary>
    public static bool IsSendfileAvailable => LazySendfileAvailable.Value;

    /// <summary>
    /// Indicates if splice is available.
    /// </summary>
    public static bool IsSpliceAvailable => LazySpliceAvailable.Value;

    /// <summary>
    /// Maximum bytes per sendfile call (for avoiding long kernel hold times).
    /// </summary>
    public const long MaxSendfileChunk = 16 * 1024 * 1024;

    /// <summary>
    /// Default pipe buffer size for splice operations.
    /// </summary>
    public const int DefaultPipeSize = 1024 * 1024;

    [DllImport("libc", EntryPoint = "sendfile64", SetLastError = true)]
    private static extern long sendfile64(int out_fd, int in_fd, ref long offset, nuint count);

    [DllImport("libc", EntryPoint = "sendfile64", SetLastError = true)]
    private static extern long sendfile64_nooffset(int out_fd, int in_fd, IntPtr offset, nuint count);

    [DllImport("libc", EntryPoint = "splice", SetLastError = true)]
    private static extern long splice(int fd_in, ref long off_in, int fd_out, ref long off_out, nuint len, uint flags);

    [DllImport("libc", EntryPoint = "pipe2", SetLastError = true)]
    private static extern int pipe2(int[] pipefd, int flags);

    [DllImport("libc", EntryPoint = "close", SetLastError = true)]
    private static extern int close(int fd);

    [DllImport("libc", EntryPoint = "fcntl", SetLastError = true)]
    private static extern int fcntl(int fd, int cmd, int arg);

    // fcntl commands
    private const int F_SETPIPE_SZ = 1031;
    private const int F_GETPIPE_SZ = 1032;

    // pipe2 flags
    private const int O_NONBLOCK = 2048;
    private const int O_CLOEXEC = 524288;

    private static bool CheckSendfileAvailable()
    {
        if (!IsLinux)
            return false;

        try
        {
            // Try to call sendfile with invalid fds - should return -1 with EBADF
            long offset = 0;
            var result = sendfile64(-1, -1, ref offset, 0);
            // If we get here without exception, syscall is available
            return true;
        }
        catch
        {
            return false;
        }
    }

    private static bool CheckSpliceAvailable()
    {
        if (!IsLinux)
            return false;

        try
        {
            long off1 = 0, off2 = 0;
            var result = splice(-1, ref off1, -1, ref off2, 0, 0);
            return true;
        }
        catch
        {
            return false;
        }
    }

    /// <summary>
    /// Performs a zero-copy sendfile transfer.
    /// </summary>
    /// <param name="sourceFd">Source file descriptor.</param>
    /// <param name="destFd">Destination socket descriptor.</param>
    /// <param name="offset">Starting offset in source file.</param>
    /// <param name="length">Number of bytes to transfer.</param>
    /// <returns>Number of bytes transferred, or -1 on error.</returns>
    public static long Sendfile(int sourceFd, int destFd, long offset, long length)
    {
        if (!IsSendfileAvailable || sourceFd < 0 || destFd < 0)
            return -1;

        try
        {
            long totalSent = 0;
            var currentOffset = offset;
            var remaining = length;

            while (remaining > 0)
            {
                var chunkSize = Math.Min(remaining, MaxSendfileChunk);
                var sent = sendfile64(destFd, sourceFd, ref currentOffset, (nuint)chunkSize);

                if (sent <= 0)
                {
                    if (sent == 0)
                        break; // EOF

                    // Check for EAGAIN/EWOULDBLOCK
                    var errno = Marshal.GetLastWin32Error();
                    if (errno == 11) // EAGAIN
                    {
                        // Socket buffer full - caller should retry later
                        break;
                    }
                    return -1; // Error
                }

                totalSent += sent;
                remaining -= sent;
            }

            return totalSent;
        }
        catch
        {
            return -1;
        }
    }

    /// <summary>
    /// Performs zero-copy transfer using splice with a pipe buffer.
    /// This is useful when sendfile isn't applicable (e.g., for HTTP/2).
    /// </summary>
    /// <param name="sourceFd">Source file descriptor.</param>
    /// <param name="destFd">Destination socket descriptor.</param>
    /// <param name="offset">Starting offset in source file.</param>
    /// <param name="length">Number of bytes to transfer.</param>
    /// <returns>Number of bytes transferred, or -1 on error.</returns>
    public static long SpliceToPipe(int sourceFd, int destFd, long offset, long length)
    {
        if (!IsSpliceAvailable || sourceFd < 0 || destFd < 0)
            return -1;

        var pipefd = new int[2];

        try
        {
            // Create a pipe with O_CLOEXEC
            if (pipe2(pipefd, O_CLOEXEC) != 0)
                return -1;

            // Increase pipe buffer size
            fcntl(pipefd[0], F_SETPIPE_SZ, DefaultPipeSize);

            long totalTransferred = 0;
            var currentOffset = offset;
            var remaining = length;

            while (remaining > 0)
            {
                var chunkSize = Math.Min(remaining, DefaultPipeSize);

                // Splice from file to pipe
                long fileOff = currentOffset;
                long pipeOff = 0;
                var toWrite = splice(sourceFd, ref fileOff, pipefd[1], ref pipeOff,
                    (nuint)chunkSize, SPLICE_F_MOVE | SPLICE_F_MORE);

                if (toWrite <= 0)
                {
                    if (toWrite == 0)
                        break; // EOF
                    return totalTransferred > 0 ? totalTransferred : -1;
                }

                // Splice from pipe to socket
                pipeOff = 0;
                long sockOff = 0;
                var written = splice(pipefd[0], ref pipeOff, destFd, ref sockOff,
                    (nuint)toWrite, SPLICE_F_MOVE | SPLICE_F_MORE);

                if (written <= 0)
                {
                    return totalTransferred > 0 ? totalTransferred : -1;
                }

                totalTransferred += written;
                currentOffset += written;
                remaining -= written;
            }

            return totalTransferred;
        }
        catch
        {
            return -1;
        }
        finally
        {
            // Close pipe fds
            if (pipefd[0] >= 0)
                close(pipefd[0]);
            if (pipefd[1] >= 0)
                close(pipefd[1]);
        }
    }

    /// <summary>
    /// Gets the optimal transfer method for a given scenario.
    /// </summary>
    /// <param name="fileSize">Size of the file to transfer.</param>
    /// <param name="isHttps">Whether this is an HTTPS connection.</param>
    /// <param name="isHttp2">Whether this is an HTTP/2 connection.</param>
    /// <returns>The recommended transfer method.</returns>
    public static TransferMethod GetOptimalMethod(long fileSize, bool isHttps = false, bool isHttp2 = false)
    {
        if (!IsLinux)
            return TransferMethod.Standard;

        // For HTTP/2 over TLS, sendfile can't be used directly
        // because the data needs to go through the TLS layer
        if (isHttp2 || isHttps)
        {
            // For large files, splice through a pipe might still help
            if (fileSize > 1024 * 1024 && IsSpliceAvailable)
            {
                return TransferMethod.Splice;
            }
            return TransferMethod.Standard;
        }

        // For plain HTTP/1.1, sendfile is the best option
        if (IsSendfileAvailable)
        {
            return TransferMethod.Sendfile;
        }

        return TransferMethod.Standard;
    }

    /// <summary>
    /// Gets the socket file descriptor from a Socket instance.
    /// </summary>
    public static int GetSocketFd(Socket socket)
    {
        if (!IsLinux || socket == null)
            return -1;

        try
        {
            return (int)socket.Handle;
        }
        catch
        {
            return -1;
        }
    }
}

/// <summary>
/// Available transfer methods.
/// </summary>
public enum TransferMethod
{
    /// <summary>
    /// Standard copy through user space.
    /// </summary>
    Standard,

    /// <summary>
    /// Zero-copy sendfile (best for HTTP/1.1 without TLS).
    /// </summary>
    Sendfile,

    /// <summary>
    /// Splice through pipe (useful for HTTP/2 or TLS).
    /// </summary>
    Splice
}

/// <summary>
/// Extension methods for zero-copy transfers.
/// </summary>
internal static class ZeroCopyExtensions
{
    /// <summary>
    /// Applies optimal settings for zero-copy transfers.
    /// </summary>
    public static void OptimizeForZeroCopy(this FileStream stream)
    {
        if (!LinuxKernelHints.IsAvailable)
            return;

        var fd = LinuxKernelHints.GetFileDescriptor(stream);
        if (fd < 0)
            return;

        // Advise sequential access for sendfile
        LinuxKernelHints.AdviseSequential(fd);
    }

    /// <summary>
    /// Prepares a file for zero-copy transfer by prefetching data.
    /// </summary>
    public static void PrepareForTransfer(this FileStream stream, long offset, long length)
    {
        if (!LinuxKernelHints.IsAvailable)
            return;

        var fd = LinuxKernelHints.GetFileDescriptor(stream);
        if (fd < 0)
            return;

        // Use aggressive readahead
        LinuxKernelHints.AggressiveReadahead(fd, offset, Math.Min(length, 8 * 1024 * 1024));
    }
}
