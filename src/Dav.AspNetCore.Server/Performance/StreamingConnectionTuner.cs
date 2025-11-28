using System.Net.Sockets;
using System.Runtime.InteropServices;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Features;

namespace Dav.AspNetCore.Server.Performance;

/// <summary>
/// Tunes connection settings for optimal streaming performance.
/// Applies response buffering optimizations and socket tuning when available.
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
    private const int TCP_KEEPIDLE = 4;
    private const int TCP_KEEPINTVL = 5;
    private const int TCP_KEEPCNT = 6;
    private const int TCP_USER_TIMEOUT = 18;
    private const int TCP_CONGESTION = 13;
    private const int TCP_MAXSEG = 2;
    private const int TCP_WINDOW_CLAMP = 10;
    private const int TCP_FASTOPEN_CONNECT = 30;

    // Socket level options
    private const int SOL_SOCKET = 1;
    private const int SO_RCVBUF = 8;
    private const int SO_SNDBUF = 7;
    private const int SO_KEEPALIVE = 9;
    private const int SO_REUSEPORT = 15;
    private const int SO_BUSY_POLL = 46;

    // Platform detection
    private static readonly bool IsLinux = RuntimeInformation.IsOSPlatform(OSPlatform.Linux);

    // Congestion control algorithms
    private static readonly byte[] CongestionBbr = System.Text.Encoding.ASCII.GetBytes("bbr\0");
    private static readonly byte[] CongestionCubic = System.Text.Encoding.ASCII.GetBytes("cubic\0");

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
            // Primary optimization: Disable response buffering for streaming
            // This is the most impactful optimization and always available
            var bufferingFeature = context.Features.Get<IHttpResponseBodyFeature>();
            bufferingFeature?.DisableBuffering();

            // Try to get socket from connection info if available
            var socket = TryGetSocket(context);
            if (socket != null)
            {
                TuneSocket(socket, contentLength, isInitialRange);
            }
        }
        catch
        {
            // Non-critical - tuning failure doesn't affect functionality
        }
    }

    /// <summary>
    /// Attempts to get the underlying socket from the HTTP context.
    /// </summary>
    private static Socket? TryGetSocket(HttpContext context)
    {
        try
        {
            // Try to get socket through connection features
            // This may not be available depending on the server configuration
            var connectionInfo = context.Connection;
            if (connectionInfo == null)
                return null;

            // Access the underlying socket through reflection if available
            // This is a best-effort approach
            var connectionType = connectionInfo.GetType();
            var socketProperty = connectionType.GetProperty("Socket");
            if (socketProperty != null)
            {
                return socketProperty.GetValue(connectionInfo) as Socket;
            }

            return null;
        }
        catch
        {
            return null;
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
            // Disable buffering
            var bufferingFeature = context.Features.Get<IHttpResponseBodyFeature>();
            bufferingFeature?.DisableBuffering();

            // Try to tune socket
            var socket = TryGetSocket(context);
            if (socket != null)
            {
                socket.SendBufferSize = StreamingSendBufferSize;
                socket.NoDelay = true;
            }
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
            // Disable buffering
            var bufferingFeature = context.Features.Get<IHttpResponseBodyFeature>();
            bufferingFeature?.DisableBuffering();

            // Try to tune socket
            var socket = TryGetSocket(context);
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

    /// <summary>
    /// Sets a string socket option (like congestion control algorithm).
    /// </summary>
    private static void SetSocketOption(Socket socket, int level, int optionName, byte[] value)
    {
        try
        {
            socket.SetRawSocketOption(level, optionName, value);
        }
        catch
        {
            // Option may not be supported on this kernel
        }
    }

    /// <summary>
    /// Applies aggressive keep-alive settings for long-lived streaming connections.
    /// This is essential for NZB streaming where connections should stay open.
    /// </summary>
    /// <param name="socket">The socket to configure.</param>
    /// <param name="keepaliveSeconds">Keep-alive timeout in seconds.</param>
    public static void ApplyAggressiveKeepalive(Socket socket, int keepaliveSeconds = 60)
    {
        if (socket == null)
            return;

        try
        {
            // Enable TCP keep-alive
            socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true);

            if (IsLinux)
            {
                // Time before sending first keep-alive probe
                SetSocketOption(socket, SOL_TCP, TCP_KEEPIDLE, keepaliveSeconds);

                // Interval between keep-alive probes
                SetSocketOption(socket, SOL_TCP, TCP_KEEPINTVL, 10);

                // Number of probes before giving up
                SetSocketOption(socket, SOL_TCP, TCP_KEEPCNT, 5);

                // User timeout - total time before giving up on unacked data
                SetSocketOption(socket, SOL_TCP, TCP_USER_TIMEOUT, keepaliveSeconds * 1000);
            }
        }
        catch
        {
            // Non-critical
        }
    }

    /// <summary>
    /// Applies optimal settings for high-bandwidth streaming.
    /// Increases buffer sizes and uses BBR congestion control if available.
    /// </summary>
    /// <param name="socket">The socket to configure.</param>
    /// <param name="expectedBandwidthMbps">Expected bandwidth in Mbps (for buffer sizing).</param>
    public static void ApplyHighBandwidthSettings(Socket socket, int expectedBandwidthMbps = 100)
    {
        if (socket == null)
            return;

        try
        {
            // Calculate optimal buffer size based on BDP (Bandwidth-Delay Product)
            // Assuming ~50ms typical latency
            var bufferSize = (expectedBandwidthMbps * 1024 * 1024 / 8) * 50 / 1000;
            bufferSize = Math.Max(bufferSize, 256 * 1024);  // Minimum 256KB
            bufferSize = Math.Min(bufferSize, 16 * 1024 * 1024);  // Maximum 16MB

            socket.ReceiveBufferSize = bufferSize;
            socket.SendBufferSize = bufferSize;

            if (IsLinux)
            {
                // Try to use BBR congestion control (better for streaming)
                SetSocketOption(socket, SOL_TCP, TCP_CONGESTION, CongestionBbr);

                // Enable busy polling for lower latency on receiving
                SetSocketOption(socket, SOL_SOCKET, SO_BUSY_POLL, 50); // 50 microseconds
            }
        }
        catch
        {
            // Non-critical
        }
    }

    /// <summary>
    /// Warms up a connection for optimal streaming performance.
    /// Call this before starting a large transfer.
    /// </summary>
    /// <param name="context">The HTTP context.</param>
    /// <param name="expectedSize">Expected transfer size.</param>
    public static void WarmupConnection(HttpContext context, long expectedSize)
    {
        try
        {
            // Disable response buffering
            var bufferingFeature = context.Features.Get<IHttpResponseBodyFeature>();
            bufferingFeature?.DisableBuffering();

            var socket = TryGetSocket(context);
            if (socket == null)
                return;

            // Configure for streaming
            socket.NoDelay = true;

            // Set buffer sizes based on transfer size
            var sendBufferSize = expectedSize switch
            {
                > 100 * 1024 * 1024 => 1024 * 1024,   // 1MB for 100MB+ transfers
                > 10 * 1024 * 1024 => 512 * 1024,    // 512KB for 10MB+ transfers
                > 1024 * 1024 => 256 * 1024,         // 256KB for 1MB+ transfers
                _ => 64 * 1024                        // 64KB for smaller transfers
            };

            socket.SendBufferSize = sendBufferSize;

            if (IsLinux)
            {
                // Apply streaming optimizations
                SetSocketOption(socket, SOL_TCP, TCP_NOTSENT_LOWAT, 16 * 1024);

                // For large transfers, use BBR
                if (expectedSize > 10 * 1024 * 1024)
                {
                    SetSocketOption(socket, SOL_TCP, TCP_CONGESTION, CongestionBbr);
                }

                // Apply keep-alive for long transfers
                if (expectedSize > 50 * 1024 * 1024)
                {
                    ApplyAggressiveKeepalive(socket, 120);
                }
            }
        }
        catch
        {
            // Non-critical
        }
    }

    /// <summary>
    /// Applies ultra-low-latency settings for initial range requests.
    /// Prioritizes TTFB over throughput.
    /// </summary>
    /// <param name="socket">The socket to configure.</param>
    public static void ApplyUltraLowLatencySettings(Socket socket)
    {
        if (socket == null)
            return;

        try
        {
            // Disable Nagle's algorithm
            socket.NoDelay = true;

            // Small send buffer for immediate transmission
            socket.SendBufferSize = 32 * 1024;

            if (IsLinux)
            {
                // Force immediate ACK
                SetSocketOption(socket, SOL_TCP, TCP_QUICKACK, 1);

                // Very low watermark for immediate send
                SetSocketOption(socket, SOL_TCP, TCP_NOTSENT_LOWAT, 4 * 1024);

                // Enable busy polling for minimal latency
                SetSocketOption(socket, SOL_SOCKET, SO_BUSY_POLL, 100);
            }
        }
        catch
        {
            // Non-critical
        }
    }

    /// <summary>
    /// Gets the recommended buffer size for a streaming transfer.
    /// </summary>
    /// <param name="contentLength">The content length.</param>
    /// <param name="isInitialRange">Whether this is an initial range request.</param>
    /// <returns>Recommended buffer size.</returns>
    public static int GetRecommendedBufferSize(long contentLength, bool isInitialRange)
    {
        if (isInitialRange)
        {
            // For initial ranges, use smaller buffers for faster TTFB
            return contentLength switch
            {
                <= 32 * 1024 => 8 * 1024,      // 8KB for tiny requests
                <= 128 * 1024 => 16 * 1024,   // 16KB for small requests
                <= 512 * 1024 => 32 * 1024,   // 32KB for medium requests
                _ => 64 * 1024                 // 64KB for larger initial ranges
            };
        }

        // For regular streaming, use larger buffers
        return contentLength switch
        {
            >= 100 * 1024 * 1024 => 1024 * 1024,  // 1MB for 100MB+ files
            >= 10 * 1024 * 1024 => 512 * 1024,   // 512KB for 10MB+ files
            >= 1024 * 1024 => 256 * 1024,        // 256KB for 1MB+ files
            _ => 64 * 1024                        // 64KB default
        };
    }
}
