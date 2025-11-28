using System.Net.Sockets;
using System.Runtime.InteropServices;
using Microsoft.AspNetCore.Connections;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Features;

namespace Dav.AspNetCore.Server.Performance;

/// <summary>
/// Tunes socket-level settings for optimal streaming performance.
/// Applies TCP_NODELAY, socket buffer tuning, and other optimizations.
/// </summary>
internal static class StreamingConnectionTuner
{
    /// <summary>
    /// Optimal send buffer size for streaming (256KB).
    /// Larger buffers reduce syscall overhead for large transfers.
    /// </summary>
    public const int StreamingSendBufferSize = 256 * 1024;

    /// <summary>
    /// Optimal send buffer for initial chunks (64KB).
    /// Smaller buffers for faster first-byte delivery.
    /// </summary>
    public const int InitialSendBufferSize = 64 * 1024;

    /// <summary>
    /// Threshold above which to use streaming buffer sizes.
    /// </summary>
    public const long StreamingThreshold = 1024 * 1024; // 1MB

    // Linux-specific TCP options
    private const int SOL_TCP = 6;
    private const int TCP_NODELAY = 1;
    private const int TCP_CORK = 3;
    private const int TCP_QUICKACK = 12;
    private const int TCP_NOTSENT_LOWAT = 25;

    // Platform detection
    private static readonly bool IsLinux = RuntimeInformation.IsOSPlatform(OSPlatform.Linux);

    /// <summary>
    /// Applies streaming optimizations to the HTTP context's connection.
    /// </summary>
    /// <param name="context">The HTTP context.</param>
    /// <param name="contentLength">The content length being streamed.</param>
    /// <param name="isInitialRange">Whether this is an initial range request.</param>
    public static void TuneForStreaming(HttpContext context, long contentLength, bool isInitialRange = false)
    {
        try
        {
            var connectionFeature = context.Features.Get<IConnectionSocketFeature>();
            var socket = connectionFeature?.Socket;

            if (socket != null)
            {
                TuneSocket(socket, contentLength, isInitialRange);
            }

            // Disable response buffering for streaming
            var bufferingFeature = context.Features.Get<IHttpResponseBodyFeature>();
            bufferingFeature?.DisableBuffering();
        }
        catch
        {
            // Non-critical - tuning failure doesn't affect functionality
        }
    }

    /// <summary>
    /// Tunes a socket for optimal streaming performance.
    /// </summary>
    /// <param name="socket">The socket to tune.</param>
    /// <param name="contentLength">The content length.</param>
    /// <param name="isInitialRange">Whether this is an initial range request.</param>
    public static void TuneSocket(Socket socket, long contentLength, bool isInitialRange = false)
    {
        if (socket == null)
            return;

        try
        {
            // Disable Nagle's algorithm for streaming - we want data sent immediately
            // This is critical for TTFB (time-to-first-byte)
            socket.NoDelay = true;

            // Set send buffer size based on transfer type
            var sendBufferSize = isInitialRange || contentLength < StreamingThreshold
                ? InitialSendBufferSize
                : StreamingSendBufferSize;

            socket.SendBufferSize = sendBufferSize;

            // Apply Linux-specific optimizations
            if (IsLinux)
            {
                ApplyLinuxOptimizations(socket, contentLength, isInitialRange);
            }
        }
        catch
        {
            // Non-critical - socket tuning failures don't affect functionality
        }
    }

    /// <summary>
    /// Applies Linux-specific TCP optimizations.
    /// </summary>
    private static void ApplyLinuxOptimizations(Socket socket, long contentLength, bool isInitialRange)
    {
        try
        {
            // TCP_QUICKACK: Force immediate ACK for faster response
            // Good for initial ranges where we want fast feedback
            if (isInitialRange)
            {
                SetSocketOption(socket, SOL_TCP, TCP_QUICKACK, 1);
            }

            // TCP_NOTSENT_LOWAT: Set low watermark for unsent data
            // This allows the application to be notified earlier when send buffer has room
            // Helps with flow control for streaming
            if (contentLength >= StreamingThreshold)
            {
                // Set to 16KB - we get notified when less than 16KB unsent
                SetSocketOption(socket, SOL_TCP, TCP_NOTSENT_LOWAT, 16 * 1024);
            }
        }
        catch
        {
            // These options may not be available on all kernels
        }
    }

    /// <summary>
    /// Enables TCP cork to batch small writes (Linux only).
    /// Call UncorkSocket when done batching.
    /// </summary>
    public static void CorkSocket(Socket socket)
    {
        if (!IsLinux || socket == null)
            return;

        try
        {
            SetSocketOption(socket, SOL_TCP, TCP_CORK, 1);
        }
        catch
        {
            // Non-critical
        }
    }

    /// <summary>
    /// Disables TCP cork to flush any corked data.
    /// </summary>
    public static void UncorkSocket(Socket socket)
    {
        if (!IsLinux || socket == null)
            return;

        try
        {
            SetSocketOption(socket, SOL_TCP, TCP_CORK, 0);
        }
        catch
        {
            // Non-critical
        }
    }

    /// <summary>
    /// Prepares the connection for a large streaming transfer.
    /// </summary>
    public static void PrepareForLargeTransfer(HttpContext context)
    {
        try
        {
            // Increase send buffer for large transfers
            var connectionFeature = context.Features.Get<IConnectionSocketFeature>();
            var socket = connectionFeature?.Socket;

            if (socket != null)
            {
                socket.SendBufferSize = StreamingSendBufferSize;
                socket.NoDelay = true;
            }

            // Disable buffering
            var bufferingFeature = context.Features.Get<IHttpResponseBodyFeature>();
            bufferingFeature?.DisableBuffering();
        }
        catch
        {
            // Non-critical
        }
    }

    /// <summary>
    /// Prepares the connection for fast initial response.
    /// </summary>
    public static void PrepareForFastFirstByte(HttpContext context)
    {
        try
        {
            var connectionFeature = context.Features.Get<IConnectionSocketFeature>();
            var socket = connectionFeature?.Socket;

            if (socket != null)
            {
                // Small buffer + no delay = fastest first byte
                socket.SendBufferSize = InitialSendBufferSize;
                socket.NoDelay = true;

                if (IsLinux)
                {
                    SetSocketOption(socket, SOL_TCP, TCP_QUICKACK, 1);
                }
            }

            var bufferingFeature = context.Features.Get<IHttpResponseBodyFeature>();
            bufferingFeature?.DisableBuffering();
        }
        catch
        {
            // Non-critical
        }
    }

    /// <summary>
    /// Sets a socket option using the native API.
    /// </summary>
    private static void SetSocketOption(Socket socket, int level, int optionName, int value)
    {
        try
        {
            socket.SetRawSocketOption(level, optionName, BitConverter.GetBytes(value));
        }
        catch
        {
            // Option may not be supported on this kernel
        }
    }
}
