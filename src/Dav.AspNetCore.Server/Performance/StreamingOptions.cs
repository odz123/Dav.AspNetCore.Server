namespace Dav.AspNetCore.Server.Performance;

/// <summary>
/// Configuration options for streaming optimizations.
/// Use these settings to fine-tune WebDAV streaming performance for your workload.
/// </summary>
public class StreamingOptions
{
    /// <summary>
    /// Files larger than this threshold will use fast metadata-based ETags
    /// instead of content hashing. Default is 10MB.
    /// Set to 0 to always use fast ETags.
    /// Set to long.MaxValue to always use content-based ETags.
    /// </summary>
    public long FastETagThreshold { get; set; } = 10 * 1024 * 1024;

    /// <summary>
    /// When true, always uses fast metadata-based ETags regardless of file size.
    /// This provides the best performance for streaming-heavy workloads.
    /// Default is false.
    /// </summary>
    public bool AlwaysUseFastETag { get; set; }

    /// <summary>
    /// Files larger than this threshold will use streaming-optimized buffer sizes (1MB).
    /// Default is 50MB.
    /// </summary>
    public long StreamingBufferThreshold { get; set; } = 50 * 1024 * 1024;

    /// <summary>
    /// Buffer size for streaming large files (default 1MB).
    /// Larger buffers can improve throughput but use more memory.
    /// </summary>
    public int StreamingBufferSize { get; set; } = 1024 * 1024;

    /// <summary>
    /// Buffer size for medium files (default 256KB).
    /// </summary>
    public int LargeBufferSize { get; set; } = 256 * 1024;

    /// <summary>
    /// Buffer size for small files (default 64KB).
    /// </summary>
    public int DefaultBufferSize { get; set; } = 64 * 1024;

    /// <summary>
    /// When true, enables SendFile optimization for physical files (zero-copy transfers).
    /// This leverages OS-level optimizations for maximum throughput.
    /// Default is true.
    /// </summary>
    public bool EnableSendFileOptimization { get; set; } = true;

    /// <summary>
    /// Cache-Control max-age in seconds for streamable content.
    /// Default is 3600 (1 hour).
    /// </summary>
    public int CacheControlMaxAge { get; set; } = 3600;

    /// <summary>
    /// Keep-Alive timeout in seconds for large file transfers.
    /// Default is 120 seconds.
    /// </summary>
    public int KeepAliveTimeout { get; set; } = 120;

    /// <summary>
    /// When true, uses OS-level read-ahead hints for sequential file access.
    /// This can significantly improve streaming performance on Linux (sendfile) and Windows.
    /// Default is true.
    /// </summary>
    public bool EnableReadAhead { get; set; } = true;

    /// <summary>
    /// When true, uses random access hints for range requests (seeking).
    /// This disables read-ahead and optimizes for random I/O patterns.
    /// Default is true.
    /// </summary>
    public bool EnableRandomAccessHints { get; set; } = true;

    /// <summary>
    /// Applies the configuration to the static caches and pools.
    /// Call this during application startup.
    /// </summary>
    public void Apply()
    {
        ETagCache.FastETagThreshold = FastETagThreshold;
        ETagCache.AlwaysUseFastETag = AlwaysUseFastETag;

        // Note: BufferPool constants cannot be changed at runtime.
        // For custom buffer sizes, use a custom implementation.
    }

    /// <summary>
    /// Creates a configuration optimized for NZB/Usenet streaming.
    /// Prioritizes fast stream starts and efficient seeking.
    /// Key optimizations:
    /// - Fast ETags (no content hashing)
    /// - Large buffers for sequential streaming
    /// - Smaller buffers for range requests (seeking)
    /// - OS-level read-ahead hints
    /// - Zero-copy file transfers
    /// </summary>
    public static StreamingOptions ForNzbStreaming() => new()
    {
        // Skip content hashing - use metadata-based ETags for instant responses
        AlwaysUseFastETag = true,
        FastETagThreshold = 0,

        // Aggressive buffering for streaming throughput
        StreamingBufferThreshold = 10 * 1024 * 1024, // 10MB
        StreamingBufferSize = 1024 * 1024, // 1MB buffers

        // Enable all OS-level optimizations
        EnableSendFileOptimization = true,
        EnableReadAhead = true,
        EnableRandomAccessHints = true,

        // Long-lived connections for streaming
        CacheControlMaxAge = 3600,
        KeepAliveTimeout = 300
    };

    /// <summary>
    /// Creates a configuration optimized for video streaming.
    /// Uses fast ETags and supports range requests efficiently.
    /// </summary>
    public static StreamingOptions ForVideoStreaming() => new()
    {
        AlwaysUseFastETag = true,
        FastETagThreshold = 0,
        StreamingBufferThreshold = 50 * 1024 * 1024, // 50MB
        StreamingBufferSize = 1024 * 1024, // 1MB
        EnableSendFileOptimization = true,
        EnableReadAhead = true,
        EnableRandomAccessHints = true,
        CacheControlMaxAge = 86400, // 24 hours
        KeepAliveTimeout = 300
    };

    /// <summary>
    /// Creates a configuration for general file serving with content validation.
    /// Uses content-based ETags for smaller files.
    /// </summary>
    public static StreamingOptions ForGeneralFileServing() => new()
    {
        AlwaysUseFastETag = false,
        FastETagThreshold = 10 * 1024 * 1024, // 10MB
        StreamingBufferThreshold = 50 * 1024 * 1024, // 50MB
        StreamingBufferSize = 256 * 1024, // 256KB
        EnableSendFileOptimization = true,
        EnableReadAhead = true,
        EnableRandomAccessHints = true,
        CacheControlMaxAge = 3600,
        KeepAliveTimeout = 120
    };

    /// <summary>
    /// Creates a configuration optimized for minimal latency.
    /// Prioritizes time-to-first-byte over throughput.
    /// </summary>
    public static StreamingOptions ForLowLatency() => new()
    {
        // Instant ETag responses
        AlwaysUseFastETag = true,
        FastETagThreshold = 0,

        // Smaller buffers for faster first-byte delivery
        StreamingBufferThreshold = 1024 * 1024, // 1MB
        StreamingBufferSize = 64 * 1024, // 64KB
        LargeBufferSize = 64 * 1024,
        DefaultBufferSize = 32 * 1024,

        // Enable optimizations
        EnableSendFileOptimization = true,
        EnableReadAhead = false, // Disable for lower latency
        EnableRandomAccessHints = true,

        // Shorter timeouts for faster error detection
        CacheControlMaxAge = 1800,
        KeepAliveTimeout = 60
    };
}
