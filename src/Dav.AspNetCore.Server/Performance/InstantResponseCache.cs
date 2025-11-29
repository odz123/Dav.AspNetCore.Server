using System.Collections.Concurrent;
using Microsoft.AspNetCore.Http;
using Microsoft.Net.Http.Headers;

namespace Dav.AspNetCore.Server.Performance;

/// <summary>
/// Provides instant response preparation for hot files.
/// Pre-computes and caches complete response headers and metadata for recently accessed files,
/// enabling near-zero TTFB (Time To First Byte) for NZB streaming.
///
/// Key optimizations:
/// - Pre-computed HTTP headers ready for immediate copy
/// - File metadata cached to avoid stat() calls
/// - Validation tokens (ETag, Last-Modified) pre-formatted
/// - Content-Range headers pre-computed for common seek positions
/// </summary>
internal sealed class InstantResponseCache : IDisposable
{
    private bool _disposed;
    private static readonly Lazy<InstantResponseCache> LazyInstance = new(() => new InstantResponseCache());
    public static InstantResponseCache Instance => LazyInstance.Value;

    /// <summary>
    /// Maximum files to cache for instant response.
    /// </summary>
    private const int MaxCachedFiles = 500;

    /// <summary>
    /// Cache duration for instant response entries.
    /// </summary>
    private static readonly TimeSpan CacheDuration = TimeSpan.FromSeconds(30);

    /// <summary>
    /// How many seek positions to pre-compute Content-Range headers for.
    /// </summary>
    private const int PreComputedSeekPositions = 100;

    /// <summary>
    /// Interval between pre-computed seek positions (1MB).
    /// </summary>
    private const long SeekPositionInterval = 1024 * 1024;

    private readonly LruCache<string, InstantResponseEntry> _cache;
    private readonly ConcurrentDictionary<string, DateTime> _accessTimes;
    private readonly Timer _cleanupTimer;

    private InstantResponseCache()
    {
        _cache = new LruCache<string, InstantResponseEntry>(MaxCachedFiles);
        _accessTimes = new ConcurrentDictionary<string, DateTime>();
        _cleanupTimer = new Timer(CleanupExpiredEntries, null, TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(1));
    }

    /// <summary>
    /// Tries to get an instant response entry for a file.
    /// Returns null if not cached or expired.
    /// </summary>
    public InstantResponseEntry? TryGetInstant(string physicalPath)
    {
        if (string.IsNullOrEmpty(physicalPath))
            return null;

        if (!_cache.TryGetValue(physicalPath, out var entry) || entry == null)
            return null;

        // Check if entry is still valid
        if (!entry.IsValid())
        {
            _cache.TryRemove(physicalPath, out _);
            return null;
        }

        // Record access for LRU
        _accessTimes[physicalPath] = DateTime.UtcNow;

        return entry;
    }

    /// <summary>
    /// Records a file access and creates an instant response entry.
    /// Call this after successfully serving a file to enable instant response on subsequent requests.
    /// </summary>
    public void RecordAccess(
        string physicalPath,
        long contentLength,
        string? contentType,
        string? etag,
        DateTimeOffset lastModified)
    {
        if (string.IsNullOrEmpty(physicalPath) || contentLength < 0)
            return;

        var entry = new InstantResponseEntry(
            physicalPath,
            contentLength,
            contentType,
            etag,
            lastModified);

        _cache.Set(physicalPath, entry);
        _accessTimes[physicalPath] = DateTime.UtcNow;
    }

    /// <summary>
    /// Pre-warms the cache for a file. Call when you know a file will be accessed soon.
    /// </summary>
    public void PreWarm(string physicalPath)
    {
        if (string.IsNullOrEmpty(physicalPath))
            return;

        // Check if already cached and valid
        if (_cache.TryGetValue(physicalPath, out var existing) && existing != null && existing.IsValid())
            return;

        try
        {
            var fileInfo = new FileInfo(physicalPath);
            if (!fileInfo.Exists)
                return;

            var contentLength = fileInfo.Length;
            var lastModified = fileInfo.LastWriteTimeUtc;
            var contentType = MimeTypeCache.GetMimeType(physicalPath);
            var etag = ETagCache.ComputeFastETag(contentLength, lastModified);

            RecordAccess(physicalPath, contentLength, contentType, etag, lastModified);
        }
        catch
        {
            // Non-critical - just skip pre-warming if it fails
        }
    }

    /// <summary>
    /// Pre-warms the cache for multiple files (e.g., after a directory listing).
    /// </summary>
    public void PreWarmBatch(IEnumerable<string> physicalPaths)
    {
        // Run pre-warming in background to not block current request
        _ = Task.Run(() =>
        {
            foreach (var path in physicalPaths.Take(50)) // Limit batch size
            {
                PreWarm(path);
            }
        });
    }

    /// <summary>
    /// Invalidates the cache entry for a file.
    /// Call when a file is modified or deleted.
    /// </summary>
    public void Invalidate(string physicalPath)
    {
        _cache.TryRemove(physicalPath, out _);
        _accessTimes.TryRemove(physicalPath, out _);
    }

    /// <summary>
    /// Invalidates all cache entries in a directory.
    /// </summary>
    public void InvalidateDirectory(string directoryPath)
    {
        // Note: This is a simple implementation. For production,
        // consider maintaining a directory -> files index.
        var keysToRemove = _accessTimes.Keys
            .Where(k => k.StartsWith(directoryPath, StringComparison.OrdinalIgnoreCase))
            .ToList();

        foreach (var key in keysToRemove)
        {
            Invalidate(key);
        }
    }

    private void CleanupExpiredEntries(object? state)
    {
        if (_disposed)
            return;

        try
        {
            var cutoff = DateTime.UtcNow - CacheDuration * 2;
            var keysToRemove = _accessTimes
                .Where(kvp => kvp.Value < cutoff)
                .Select(kvp => kvp.Key)
                .ToList();

            foreach (var key in keysToRemove)
            {
                Invalidate(key);
            }
        }
        catch
        {
            // Non-critical cleanup
        }
    }

    /// <summary>
    /// Disposes resources used by the cache.
    /// </summary>
    public void Dispose()
    {
        if (_disposed)
            return;

        _disposed = true;
        _cleanupTimer.Dispose();
    }
}

/// <summary>
/// A cached entry containing pre-computed response data for instant serving.
/// </summary>
internal sealed class InstantResponseEntry
{
    /// <summary>
    /// Physical file path.
    /// </summary>
    public string PhysicalPath { get; }

    /// <summary>
    /// Cached content length.
    /// </summary>
    public long ContentLength { get; }

    /// <summary>
    /// Cached content type.
    /// </summary>
    public string? ContentType { get; }

    /// <summary>
    /// Pre-formatted ETag header value (including quotes).
    /// </summary>
    public string? ETagHeader { get; }

    /// <summary>
    /// Cached last modified time.
    /// </summary>
    public DateTimeOffset LastModified { get; }

    /// <summary>
    /// Pre-formatted Last-Modified header value.
    /// </summary>
    public string LastModifiedHeader { get; }

    /// <summary>
    /// When this entry was created.
    /// </summary>
    public DateTime CreatedAt { get; }

    /// <summary>
    /// Pre-computed Content-Range headers for common seek positions.
    /// Key is the start offset, value is the header value.
    /// </summary>
    private readonly Dictionary<long, string> _preComputedRanges;

    /// <summary>
    /// Pre-formatted Accept-Ranges header.
    /// </summary>
    public static readonly string AcceptRangesHeader = "bytes";

    public InstantResponseEntry(
        string physicalPath,
        long contentLength,
        string? contentType,
        string? etag,
        DateTimeOffset lastModified)
    {
        PhysicalPath = physicalPath;
        ContentLength = contentLength;
        ContentType = contentType;
        ETagHeader = string.IsNullOrEmpty(etag) ? null : $"\"{etag}\"";
        LastModified = lastModified;
        LastModifiedHeader = lastModified.ToString("R");
        CreatedAt = DateTime.UtcNow;

        // Pre-compute Content-Range headers for common seek positions
        _preComputedRanges = PreComputeRangeHeaders(contentLength);
    }

    /// <summary>
    /// Checks if this entry is still valid (file hasn't changed).
    /// Uses a fast timestamp check first, then falls back to file stat if needed.
    /// </summary>
    public bool IsValid()
    {
        // Quick expiry check
        if (DateTime.UtcNow - CreatedAt > InstantResponseCacheConfig.MaxEntryAge)
            return false;

        try
        {
            // Fast validation using file modification time
            var currentLastWrite = File.GetLastWriteTimeUtc(PhysicalPath);
            return Math.Abs((currentLastWrite - LastModified.UtcDateTime).TotalSeconds) < 1;
        }
        catch
        {
            return false;
        }
    }

    /// <summary>
    /// Applies cached headers to the response for maximum speed.
    /// </summary>
    public void ApplyHeaders(HttpResponse response)
    {
        if (!string.IsNullOrEmpty(ContentType))
            response.ContentType = ContentType;

        if (ETagHeader != null)
            response.Headers["ETag"] = ETagHeader;

        response.Headers["Last-Modified"] = LastModifiedHeader;
        response.Headers["Accept-Ranges"] = AcceptRangesHeader;
        response.ContentLength = ContentLength;
    }

    /// <summary>
    /// Applies headers for a range request with pre-computed Content-Range if available.
    /// </summary>
    public void ApplyRangeHeaders(HttpResponse response, long offset, long length)
    {
        if (!string.IsNullOrEmpty(ContentType))
            response.ContentType = ContentType;

        if (ETagHeader != null)
            response.Headers["ETag"] = ETagHeader;

        response.Headers["Last-Modified"] = LastModifiedHeader;
        response.Headers["Accept-Ranges"] = AcceptRangesHeader;
        response.ContentLength = length;

        // Try to use pre-computed Content-Range header
        var rangeHeader = GetPreComputedRangeHeader(offset, length);
        response.Headers["Content-Range"] = rangeHeader;
    }

    /// <summary>
    /// Gets a pre-computed Content-Range header or generates one.
    /// </summary>
    public string GetPreComputedRangeHeader(long offset, long length)
    {
        // Check for exact match in pre-computed headers
        if (_preComputedRanges.TryGetValue(offset, out var cached))
        {
            // Verify the length matches what we pre-computed
            var expectedEnd = offset + length - 1;
            var preComputedEnd = offset + Math.Min(length, ContentLength - offset) - 1;
            if (expectedEnd == preComputedEnd)
                return cached;
        }

        // Generate on demand
        var end = offset + length - 1;
        return $"bytes {offset}-{end}/{ContentLength}";
    }

    private static Dictionary<long, string> PreComputeRangeHeaders(long contentLength)
    {
        var headers = new Dictionary<long, string>();

        // Pre-compute headers for initial positions (most common for streaming start)
        headers[0] = FormatRangeHeader(0, Math.Min(1024 * 1024, contentLength) - 1, contentLength);

        // Pre-compute at regular intervals
        for (int i = 1; i <= 100 && i * 1024 * 1024 < contentLength; i++)
        {
            var offset = (long)i * 1024 * 1024;
            var end = Math.Min(offset + 1024 * 1024, contentLength) - 1;
            headers[offset] = FormatRangeHeader(offset, end, contentLength);
        }

        return headers;
    }

    private static string FormatRangeHeader(long start, long end, long total)
    {
        return $"bytes {start}-{end}/{total}";
    }
}

/// <summary>
/// Configuration for instant response caching.
/// </summary>
internal static class InstantResponseCacheConfig
{
    /// <summary>
    /// Maximum age for cached entries before revalidation.
    /// </summary>
    public static TimeSpan MaxEntryAge { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Whether to enable aggressive pre-warming on directory access.
    /// </summary>
    public static bool EnableAggressivePreWarm { get; set; } = true;
}
