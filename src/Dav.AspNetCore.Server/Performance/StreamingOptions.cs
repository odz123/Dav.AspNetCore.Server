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
    /// Uses fast ETags and maximum buffer sizes.
    /// </summary>
    public static StreamingOptions ForNzbStreaming() => new()
    {
        AlwaysUseFastETag = true,
        FastETagThreshold = 0,
        StreamingBufferThreshold = 10 * 1024 * 1024, // 10MB
        StreamingBufferSize = 1024 * 1024, // 1MB
        EnableSendFileOptimization = true,
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
        CacheControlMaxAge = 3600,
        KeepAliveTimeout = 120
    };
}
