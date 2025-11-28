using System.Collections.Concurrent;

namespace Dav.AspNetCore.Server.Performance;

/// <summary>
/// Caches file metadata (size, last modified, content type) to reduce I/O before first byte.
/// This significantly speeds up the initial response headers for streaming.
/// </summary>
internal sealed class FileMetadataCache
{
    private static readonly Lazy<FileMetadataCache> LazyInstance = new(() => new FileMetadataCache());
    public static FileMetadataCache Instance => LazyInstance.Value;

    /// <summary>
    /// Maximum cached entries.
    /// </summary>
    private const int MaxCacheSize = 10000;

    /// <summary>
    /// How long cached metadata is considered fresh.
    /// For streaming scenarios, we use a short TTL since content may change.
    /// </summary>
    private static readonly TimeSpan CacheTtl = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Maximum age before forced refresh.
    /// </summary>
    private static readonly TimeSpan MaxAge = TimeSpan.FromMinutes(5);

    private readonly LruCache<string, CachedMetadata> _cache;
    private readonly ConcurrentDictionary<string, SemaphoreSlim> _locks;

    private FileMetadataCache()
    {
        _cache = new LruCache<string, CachedMetadata>(MaxCacheSize);
        _locks = new ConcurrentDictionary<string, SemaphoreSlim>();
    }

    /// <summary>
    /// Gets cached file metadata or fetches it from disk.
    /// </summary>
    /// <param name="physicalPath">The physical file path.</param>
    /// <returns>Cached or fresh file metadata.</returns>
    public CachedMetadata? GetMetadata(string physicalPath)
    {
        if (string.IsNullOrEmpty(physicalPath))
            return null;

        var now = DateTime.UtcNow;

        // Try to get from cache
        if (_cache.TryGetValue(physicalPath, out var cached) && cached != null)
        {
            // Check if still fresh
            if (now - cached.CachedAt < CacheTtl)
            {
                return cached;
            }

            // Stale but within max age - return stale and refresh in background
            if (now - cached.CachedAt < MaxAge)
            {
                // Trigger background refresh
                _ = Task.Run(() => RefreshMetadataAsync(physicalPath));
                return cached;
            }
        }

        // Not in cache or too old - fetch synchronously
        return FetchAndCacheMetadata(physicalPath);
    }

    /// <summary>
    /// Gets cached file metadata asynchronously.
    /// Prefers returning cached data immediately while refreshing in background.
    /// </summary>
    public async ValueTask<CachedMetadata?> GetMetadataAsync(string physicalPath, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrEmpty(physicalPath))
            return null;

        var now = DateTime.UtcNow;

        // Try to get from cache
        if (_cache.TryGetValue(physicalPath, out var cached) && cached != null)
        {
            if (now - cached.CachedAt < CacheTtl)
            {
                return cached;
            }

            // Stale but usable - return immediately and refresh in background
            if (now - cached.CachedAt < MaxAge)
            {
                _ = RefreshMetadataAsync(physicalPath);
                return cached;
            }
        }

        // Need fresh data - use semaphore to prevent thundering herd
        var lockObj = _locks.GetOrAdd(physicalPath, _ => new SemaphoreSlim(1, 1));

        try
        {
            await lockObj.WaitAsync(cancellationToken).ConfigureAwait(false);

            // Double-check after acquiring lock
            if (_cache.TryGetValue(physicalPath, out cached) && cached != null)
            {
                if (now - cached.CachedAt < CacheTtl)
                {
                    return cached;
                }
            }

            // Fetch from disk
            return await Task.Run(() => FetchAndCacheMetadata(physicalPath), cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            lockObj.Release();

            // Clean up lock if no longer needed
            if (lockObj.CurrentCount == 1)
            {
                _locks.TryRemove(physicalPath, out _);
            }
        }
    }

    /// <summary>
    /// Pre-populates the cache with metadata for a file.
    /// Call this proactively when you know a file will be accessed soon.
    /// </summary>
    public void Preload(string physicalPath)
    {
        if (string.IsNullOrEmpty(physicalPath))
            return;

        // Don't preload if already fresh
        if (_cache.TryGetValue(physicalPath, out var cached) && cached != null)
        {
            if (DateTime.UtcNow - cached.CachedAt < CacheTtl)
                return;
        }

        _ = Task.Run(() => FetchAndCacheMetadata(physicalPath));
    }

    /// <summary>
    /// Invalidates cached metadata for a file.
    /// </summary>
    public void Invalidate(string physicalPath)
    {
        _cache.TryRemove(physicalPath, out _);
    }

    /// <summary>
    /// Clears all cached metadata.
    /// </summary>
    public void Clear()
    {
        _cache.Clear();
    }

    private CachedMetadata? FetchAndCacheMetadata(string physicalPath)
    {
        try
        {
            var fileInfo = new FileInfo(physicalPath);
            if (!fileInfo.Exists)
                return null;

            var metadata = new CachedMetadata(
                fileInfo.Length,
                fileInfo.LastWriteTimeUtc,
                MimeTypeCache.Instance.GetMimeType(physicalPath),
                DateTime.UtcNow);

            _cache.Set(physicalPath, metadata);
            return metadata;
        }
        catch
        {
            return null;
        }
    }

    private async Task RefreshMetadataAsync(string physicalPath)
    {
        try
        {
            await Task.Run(() => FetchAndCacheMetadata(physicalPath)).ConfigureAwait(false);
        }
        catch
        {
            // Background refresh failures are non-critical
        }
    }
}

/// <summary>
/// Cached file metadata for fast access.
/// </summary>
public readonly struct CachedMetadata
{
    /// <summary>
    /// File size in bytes.
    /// </summary>
    public long Length { get; }

    /// <summary>
    /// Last modification time (UTC).
    /// </summary>
    public DateTime LastModified { get; }

    /// <summary>
    /// MIME content type.
    /// </summary>
    public string ContentType { get; }

    /// <summary>
    /// When this metadata was cached.
    /// </summary>
    public DateTime CachedAt { get; }

    /// <summary>
    /// Pre-computed ETag (metadata-based).
    /// </summary>
    public string ETag { get; }

    public CachedMetadata(long length, DateTime lastModified, string contentType, DateTime cachedAt)
    {
        Length = length;
        LastModified = lastModified;
        ContentType = contentType;
        CachedAt = cachedAt;
        ETag = ETagCache.ComputeFastETag(length, lastModified);
    }
}
