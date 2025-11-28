using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Primitives;

namespace Dav.AspNetCore.Server.Performance;

/// <summary>
/// Pre-computed response headers for faster TTFB in streaming scenarios.
/// Caches commonly used header values to avoid repeated string allocations.
/// </summary>
internal static class ResponseHeaderCache
{
    // Pre-computed static header values
    public static readonly StringValues AcceptRangesBytes = new("bytes");
    public static readonly StringValues AcceptRangesNone = new("none");
    public static readonly StringValues ConnectionKeepAlive = new("keep-alive");
    public static readonly StringValues CacheControlStreaming = new("private, max-age=3600, must-revalidate");
    public static readonly StringValues CacheControlNoCache = new("no-cache, no-store, must-revalidate");

    // Pre-computed keep-alive timeouts
    private static readonly LruCache<int, StringValues> KeepAliveCache = new(10);

    // Pre-computed content ranges (for common offsets)
    private static readonly LruCache<string, StringValues> ContentRangeCache = new(1000);

    /// <summary>
    /// Gets a pre-computed Keep-Alive header value.
    /// </summary>
    public static StringValues GetKeepAliveHeader(int timeoutSeconds)
    {
        if (KeepAliveCache.TryGetValue(timeoutSeconds, out var cached))
            return cached;

        var value = new StringValues($"timeout={timeoutSeconds}");
        KeepAliveCache.Set(timeoutSeconds, value);
        return value;
    }

    /// <summary>
    /// Gets or creates a Content-Range header value.
    /// Caches commonly requested ranges to avoid repeated string formatting.
    /// </summary>
    public static StringValues GetContentRangeHeader(long start, long end, long total)
    {
        // Only cache small ranges (first 10MB) which are common for video start
        if (start <= 10 * 1024 * 1024)
        {
            var key = $"{start}-{end}/{total}";
            if (ContentRangeCache.TryGetValue(key, out var cached))
                return cached;

            var value = new StringValues($"bytes {start}-{end}/{total}");
            ContentRangeCache.Set(key, value);
            return value;
        }

        // Don't cache large offsets - just create
        return new StringValues($"bytes {start}-{end}/{total}");
    }

    /// <summary>
    /// Gets a Content-Range header for unsatisfiable range.
    /// </summary>
    public static StringValues GetUnsatisfiableRangeHeader(long total)
    {
        return new StringValues($"bytes */{total}");
    }

    /// <summary>
    /// Applies optimized streaming headers to the response.
    /// </summary>
    public static void ApplyStreamingHeaders(IHeaderDictionary headers, long contentLength, int keepAliveSeconds = 120)
    {
        headers["Accept-Ranges"] = AcceptRangesBytes;
        headers["Cache-Control"] = CacheControlStreaming;

        if (contentLength > BufferPool.StreamingThreshold)
        {
            headers["Connection"] = ConnectionKeepAlive;
            headers["Keep-Alive"] = GetKeepAliveHeader(keepAliveSeconds);
        }
    }

    /// <summary>
    /// Pre-computed content type headers for common streaming types.
    /// </summary>
    private static readonly Dictionary<string, StringValues> CommonContentTypes = new(StringComparer.OrdinalIgnoreCase)
    {
        { ".mp4", new StringValues("video/mp4") },
        { ".mkv", new StringValues("video/x-matroska") },
        { ".webm", new StringValues("video/webm") },
        { ".avi", new StringValues("video/x-msvideo") },
        { ".mov", new StringValues("video/quicktime") },
        { ".m4v", new StringValues("video/x-m4v") },
        { ".ts", new StringValues("video/mp2t") },
        { ".m3u8", new StringValues("application/x-mpegURL") },
        { ".mp3", new StringValues("audio/mpeg") },
        { ".flac", new StringValues("audio/flac") },
        { ".aac", new StringValues("audio/aac") },
        { ".ogg", new StringValues("audio/ogg") },
        { ".nzb", new StringValues("application/x-nzb") },
        { ".rar", new StringValues("application/x-rar-compressed") },
        { ".zip", new StringValues("application/zip") },
        { ".7z", new StringValues("application/x-7z-compressed") }
    };

    /// <summary>
    /// Gets a pre-computed content type header for common file extensions.
    /// </summary>
    public static StringValues? GetContentTypeHeader(string extension)
    {
        if (CommonContentTypes.TryGetValue(extension, out var contentType))
            return contentType;
        return null;
    }
}
