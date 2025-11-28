using System.Collections.Concurrent;

namespace Dav.AspNetCore.Server.Performance;

/// <summary>
/// Implements read amplification - reading more data than requested to have it ready
/// for subsequent sequential reads. This dramatically reduces TTFB for streaming.
/// </summary>
internal sealed class ReadAmplifier : IDisposable
{
    private static readonly Lazy<ReadAmplifier> LazyInstance = new(() => new ReadAmplifier());
    public static ReadAmplifier Instance => LazyInstance.Value;

    /// <summary>
    /// Default amplification factor - read 4x requested for initial ranges.
    /// </summary>
    public const int DefaultAmplificationFactor = 4;

    /// <summary>
    /// Maximum amplification size (8MB).
    /// </summary>
    public const long MaxAmplificationSize = 8 * 1024 * 1024;

    /// <summary>
    /// Minimum size to amplify (16KB).
    /// </summary>
    public const long MinAmplificationThreshold = 16 * 1024;

    /// <summary>
    /// Maximum cached data per file.
    /// </summary>
    public const long MaxCachedPerFile = 16 * 1024 * 1024;

    /// <summary>
    /// Maximum number of files to cache.
    /// </summary>
    private const int MaxCachedFiles = 100;

    /// <summary>
    /// How long to keep amplified data.
    /// </summary>
    private static readonly TimeSpan CacheExpiration = TimeSpan.FromSeconds(30);

    private readonly LruCache<string, AmplifiedFileCache> _cache;
    private readonly Timer _cleanupTimer;
    private bool _disposed;

    private ReadAmplifier()
    {
        _cache = new LruCache<string, AmplifiedFileCache>(MaxCachedFiles);
        _cleanupTimer = new Timer(CleanupCallback, null,
            TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(10));
    }

    /// <summary>
    /// Gets the optimal amplification settings for a request.
    /// </summary>
    /// <param name="requestedOffset">The requested offset.</param>
    /// <param name="requestedLength">The requested length.</param>
    /// <param name="fileSize">The total file size.</param>
    /// <returns>Amplification settings with actual offset/length to read.</returns>
    public AmplificationSettings GetSettings(long requestedOffset, long requestedLength, long fileSize)
    {
        // Don't amplify small requests or requests near end of file
        if (requestedLength < MinAmplificationThreshold)
        {
            return new AmplificationSettings(requestedOffset, requestedLength, false);
        }

        var remainingFile = fileSize - requestedOffset;
        if (remainingFile <= requestedLength * 2)
        {
            // Near end of file - don't amplify
            return new AmplificationSettings(requestedOffset, remainingFile, false);
        }

        // Calculate amplified length
        long amplifiedLength;

        // For initial ranges (first 2MB), be more aggressive
        if (requestedOffset < 2 * 1024 * 1024)
        {
            // Read up to 8MB for initial ranges
            amplifiedLength = Math.Min(MaxAmplificationSize, remainingFile);
        }
        else
        {
            // Standard amplification: 4x the request, capped at 4MB
            amplifiedLength = Math.Min(
                requestedLength * DefaultAmplificationFactor,
                Math.Min(4 * 1024 * 1024, remainingFile));
        }

        return new AmplificationSettings(requestedOffset, amplifiedLength, amplifiedLength > requestedLength);
    }

    /// <summary>
    /// Caches amplified data for future reads.
    /// </summary>
    /// <param name="filePath">The file path.</param>
    /// <param name="offset">The starting offset of the cached data.</param>
    /// <param name="data">The amplified data.</param>
    public void CacheAmplifiedData(string filePath, long offset, ReadOnlyMemory<byte> data)
    {
        if (_disposed || data.Length == 0)
            return;

        var fileCache = _cache.GetOrAdd(filePath, _ => new AmplifiedFileCache(filePath));
        fileCache.AddSegment(offset, data);
    }

    /// <summary>
    /// Tries to get cached data for a range request.
    /// </summary>
    /// <param name="filePath">The file path.</param>
    /// <param name="offset">The requested offset.</param>
    /// <param name="length">The requested length.</param>
    /// <param name="data">The cached data if available.</param>
    /// <returns>True if the entire requested range is cached.</returns>
    public bool TryGetCachedData(string filePath, long offset, long length, out ReadOnlyMemory<byte> data)
    {
        data = default;

        if (_disposed)
            return false;

        if (_cache.TryGetValue(filePath, out var fileCache) && fileCache != null)
        {
            return fileCache.TryGetRange(offset, length, out data);
        }

        return false;
    }

    /// <summary>
    /// Checks if a range is fully or partially cached.
    /// </summary>
    public CacheStatus GetCacheStatus(string filePath, long offset, long length)
    {
        if (_disposed)
            return CacheStatus.NotCached;

        if (_cache.TryGetValue(filePath, out var fileCache) && fileCache != null)
        {
            return fileCache.GetRangeStatus(offset, length);
        }

        return CacheStatus.NotCached;
    }

    /// <summary>
    /// Invalidates cache for a file.
    /// </summary>
    public void Invalidate(string filePath)
    {
        if (_cache.TryRemove(filePath, out var fileCache) && fileCache != null)
        {
            fileCache.Clear();
        }
    }

    private void CleanupCallback(object? state)
    {
        if (_disposed)
            return;

        try
        {
            var now = DateTime.UtcNow;
            var keysToRemove = new List<string>();

            foreach (var key in _cache.Keys)
            {
                if (_cache.TryGetValue(key, out var fileCache) && fileCache != null)
                {
                    if ((now - fileCache.LastAccess) > CacheExpiration)
                    {
                        keysToRemove.Add(key);
                    }
                    else
                    {
                        // Trim old segments
                        fileCache.TrimExpired(now - CacheExpiration);
                    }
                }
            }

            foreach (var key in keysToRemove)
            {
                if (_cache.TryRemove(key, out var fileCache) && fileCache != null)
                {
                    fileCache.Clear();
                }
            }
        }
        catch
        {
            // Cleanup errors are non-critical
        }
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        _disposed = true;
        _cleanupTimer.Dispose();

        foreach (var key in _cache.Keys.ToList())
        {
            if (_cache.TryRemove(key, out var fileCache) && fileCache != null)
            {
                fileCache.Clear();
            }
        }
    }

    /// <summary>
    /// Cached data for a single file.
    /// </summary>
    private sealed class AmplifiedFileCache
    {
        public string FilePath { get; }
        public DateTime LastAccess { get; private set; }

        private readonly SortedList<long, CachedSegment> _segments;
        private readonly object _lock = new();
        private long _totalSize;

        public AmplifiedFileCache(string filePath)
        {
            FilePath = filePath;
            LastAccess = DateTime.UtcNow;
            _segments = new SortedList<long, CachedSegment>();
        }

        public void AddSegment(long offset, ReadOnlyMemory<byte> data)
        {
            lock (_lock)
            {
                LastAccess = DateTime.UtcNow;

                // Evict old data if we're over the limit
                while (_totalSize + data.Length > MaxCachedPerFile && _segments.Count > 0)
                {
                    var oldest = _segments.Values[0];
                    _segments.RemoveAt(0);
                    _totalSize -= oldest.Data.Length;
                }

                // Add new segment
                var segment = new CachedSegment(offset, data.ToArray(), DateTime.UtcNow);
                _segments[offset] = segment;
                _totalSize += data.Length;
            }
        }

        public bool TryGetRange(long offset, long length, out ReadOnlyMemory<byte> data)
        {
            data = default;

            lock (_lock)
            {
                LastAccess = DateTime.UtcNow;

                // Find segment that contains the requested range
                foreach (var kvp in _segments)
                {
                    var segment = kvp.Value;
                    var segmentEnd = segment.Offset + segment.Data.Length;

                    if (segment.Offset <= offset && segmentEnd >= offset + length)
                    {
                        // Range is fully within this segment
                        var startIndex = (int)(offset - segment.Offset);
                        data = new ReadOnlyMemory<byte>(segment.Data, startIndex, (int)length);
                        return true;
                    }
                }
            }

            return false;
        }

        public CacheStatus GetRangeStatus(long offset, long length)
        {
            lock (_lock)
            {
                var requestEnd = offset + length;

                foreach (var kvp in _segments)
                {
                    var segment = kvp.Value;
                    var segmentEnd = segment.Offset + segment.Data.Length;

                    // Fully cached
                    if (segment.Offset <= offset && segmentEnd >= requestEnd)
                    {
                        return CacheStatus.FullyCached;
                    }

                    // Partially cached (overlapping)
                    if (segment.Offset < requestEnd && segmentEnd > offset)
                    {
                        return CacheStatus.PartiallyCached;
                    }
                }
            }

            return CacheStatus.NotCached;
        }

        public void TrimExpired(DateTime cutoff)
        {
            lock (_lock)
            {
                var keysToRemove = new List<long>();

                foreach (var kvp in _segments)
                {
                    if (kvp.Value.CreatedAt < cutoff)
                    {
                        keysToRemove.Add(kvp.Key);
                    }
                }

                foreach (var key in keysToRemove)
                {
                    if (_segments.TryGetValue(key, out var segment))
                    {
                        _totalSize -= segment.Data.Length;
                        _segments.Remove(key);
                    }
                }
            }
        }

        public void Clear()
        {
            lock (_lock)
            {
                _segments.Clear();
                _totalSize = 0;
            }
        }

        private readonly struct CachedSegment
        {
            public long Offset { get; }
            public byte[] Data { get; }
            public DateTime CreatedAt { get; }

            public CachedSegment(long offset, byte[] data, DateTime createdAt)
            {
                Offset = offset;
                Data = data;
                CreatedAt = createdAt;
            }
        }
    }
}

/// <summary>
/// Settings for read amplification.
/// </summary>
public readonly struct AmplificationSettings
{
    /// <summary>
    /// The offset to start reading from.
    /// </summary>
    public long Offset { get; }

    /// <summary>
    /// The length to read (may be amplified beyond the original request).
    /// </summary>
    public long Length { get; }

    /// <summary>
    /// Whether amplification was applied.
    /// </summary>
    public bool IsAmplified { get; }

    public AmplificationSettings(long offset, long length, bool isAmplified)
    {
        Offset = offset;
        Length = length;
        IsAmplified = isAmplified;
    }
}

/// <summary>
/// Status of cached data for a range.
/// </summary>
public enum CacheStatus
{
    /// <summary>
    /// Range is not in cache.
    /// </summary>
    NotCached,

    /// <summary>
    /// Range is partially in cache.
    /// </summary>
    PartiallyCached,

    /// <summary>
    /// Range is fully in cache.
    /// </summary>
    FullyCached
}
