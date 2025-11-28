using System.Security.Cryptography;

namespace Dav.AspNetCore.Server.Performance;

/// <summary>
/// Provides fast ETag computation with caching based on file metadata.
/// </summary>
internal sealed class ETagCache
{
    private readonly LruCache<string, ETagEntry> _cache;
    private static readonly Lazy<ETagCache> LazyInstance = new(() => new ETagCache(10000));

    /// <summary>
    /// Gets the singleton instance of the ETag cache.
    /// </summary>
    public static ETagCache Instance => LazyInstance.Value;

    private sealed class ETagEntry
    {
        public string ETag { get; }
        public long FileSize { get; }
        public DateTime LastModified { get; }

        public ETagEntry(string etag, long fileSize, DateTime lastModified)
        {
            ETag = etag;
            FileSize = fileSize;
            LastModified = lastModified;
        }
    }

    /// <summary>
    /// Initializes a new instance of the ETagCache class.
    /// </summary>
    /// <param name="capacity">The maximum number of ETags to cache.</param>
    public ETagCache(int capacity)
    {
        _cache = new LruCache<string, ETagEntry>(capacity);
    }

    /// <summary>
    /// Gets or computes the ETag for a file based on its URI and metadata.
    /// Uses cached value if file hasn't changed.
    /// </summary>
    /// <param name="uri">The file URI (used as cache key).</param>
    /// <param name="fileSize">The current file size.</param>
    /// <param name="lastModified">The file's last modification time.</param>
    /// <param name="streamFactory">Factory to create a stream for computing the hash if needed.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The ETag string.</returns>
    public async ValueTask<string> GetOrComputeAsync(
        Uri uri,
        long fileSize,
        DateTime lastModified,
        Func<ValueTask<Stream>> streamFactory,
        CancellationToken cancellationToken = default)
    {
        var cacheKey = uri.AbsolutePath;

        // Check if we have a valid cached entry
        if (_cache.TryGetValue(cacheKey, out var entry))
        {
            // Verify the cached entry is still valid (file hasn't changed)
            if (entry != null &&
                entry.FileSize == fileSize &&
                entry.LastModified == lastModified)
            {
                return entry.ETag;
            }
        }

        // Compute new ETag
        var etag = await ComputeETagAsync(streamFactory, cancellationToken).ConfigureAwait(false);

        // Cache the result
        _cache.Set(cacheKey, new ETagEntry(etag, fileSize, lastModified));

        return etag;
    }

    /// <summary>
    /// Computes a fast ETag based on file metadata without reading the file.
    /// This is less precise but much faster for large files.
    /// </summary>
    /// <param name="fileSize">The file size.</param>
    /// <param name="lastModified">The last modification time.</param>
    /// <returns>A fast ETag based on metadata.</returns>
    public static string ComputeFastETag(long fileSize, DateTime lastModified)
    {
        // Create a hash from the file size and last modified timestamp
        // This is fast but will change if the file is modified
        var hashInput = $"{fileSize}-{lastModified.Ticks}";
        var hashBytes = MD5.HashData(System.Text.Encoding.UTF8.GetBytes(hashInput));
        return Convert.ToHexString(hashBytes);
    }

    /// <summary>
    /// Computes the ETag by hashing the file content.
    /// </summary>
    private static async ValueTask<string> ComputeETagAsync(
        Func<ValueTask<Stream>> streamFactory,
        CancellationToken cancellationToken)
    {
        await using var stream = await streamFactory().ConfigureAwait(false);
        var hash = await MD5.HashDataAsync(stream, cancellationToken).ConfigureAwait(false);
        return Convert.ToHexString(hash);
    }

    /// <summary>
    /// Invalidates a cached ETag for a specific URI.
    /// </summary>
    /// <param name="uri">The URI to invalidate.</param>
    public void Invalidate(Uri uri)
    {
        _cache.TryRemove(uri.AbsolutePath, out _);
    }

    /// <summary>
    /// Clears all cached ETags.
    /// </summary>
    public void Clear()
    {
        _cache.Clear();
    }
}
