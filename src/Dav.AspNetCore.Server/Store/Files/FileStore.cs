using System.Collections.Concurrent;
using Dav.AspNetCore.Server.Performance;

namespace Dav.AspNetCore.Server.Store.Files;

public abstract class FileStore : IStore
{
    /// <summary>
    /// Default cache capacity for items.
    /// </summary>
    private const int DefaultItemCacheCapacity = 10000;

    /// <summary>
    /// Default cache capacity for collections.
    /// </summary>
    private const int DefaultCollectionCacheCapacity = 1000;

    private readonly LruCache<string, IStoreItem?> _itemCache;
    private readonly LruCache<string, List<IStoreItem>> _collectionCache;

    /// <summary>
    /// Initializes a new instance of the FileStore class.
    /// </summary>
    protected FileStore()
        : this(DefaultItemCacheCapacity, DefaultCollectionCacheCapacity)
    {
    }

    /// <summary>
    /// Initializes a new instance of the FileStore class with custom cache sizes.
    /// </summary>
    /// <param name="itemCacheCapacity">The maximum number of items to cache.</param>
    /// <param name="collectionCacheCapacity">The maximum number of collection listings to cache.</param>
    protected FileStore(int itemCacheCapacity, int collectionCacheCapacity)
    {
        _itemCache = new LruCache<string, IStoreItem?>(itemCacheCapacity);
        _collectionCache = new LruCache<string, List<IStoreItem>>(collectionCacheCapacity);
    }

    /// <summary>
    /// Gets the item cache (for internal use).
    /// </summary>
    internal LruCache<string, IStoreItem?> ItemCache => _itemCache;

    /// <summary>
    /// Gets the collection cache (for internal use).
    /// </summary>
    internal LruCache<string, List<IStoreItem>> CollectionCache => _collectionCache;

    /// <summary>
    /// A value indicating whether caching will be disabled.
    /// </summary>
    public bool DisableCaching { get; set; }

    /// <summary>
    /// Gets the store item async.
    /// </summary>
    /// <param name="uri">The uri.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The store item or null.</returns>
    public async Task<IStoreItem?> GetItemAsync(Uri uri, CancellationToken cancellationToken = default)
    {
        var cacheKey = uri.AbsolutePath;

        if (!DisableCaching && _itemCache.TryGetValue(cacheKey, out var cacheItem))
            return cacheItem;

        if (await DirectoryExistsAsync(uri, cancellationToken).ConfigureAwait(false))
        {
            var directoryProperties = await GetDirectoryPropertiesAsync(uri, cancellationToken).ConfigureAwait(false);
            var directory = new Directory(this, directoryProperties);

            if (!DisableCaching)
                _itemCache.Set(cacheKey, directory);

            return directory;
        }

        if (await FileExistsAsync(uri, cancellationToken).ConfigureAwait(false))
        {
            var fileProperties = await GetFilePropertiesAsync(uri, cancellationToken).ConfigureAwait(false);
            var file = new File(this, fileProperties);

            if (!DisableCaching)
                _itemCache.Set(cacheKey, file);

            return file;
        }

        return null;
    }

    /// <summary>
    /// Invalidates the cache entry for the specified URI.
    /// </summary>
    /// <param name="uri">The URI to invalidate.</param>
    internal void InvalidateCache(Uri uri)
    {
        var cacheKey = uri.AbsolutePath;
        _itemCache.TryRemove(cacheKey, out _);
        _collectionCache.TryRemove(cacheKey, out _);

        // Also invalidate parent collection cache
        var parentPath = GetParentPath(cacheKey);
        if (!string.IsNullOrEmpty(parentPath))
            _collectionCache.TryRemove(parentPath, out _);
    }

    private static string GetParentPath(string path)
    {
        if (string.IsNullOrEmpty(path) || path == "/")
            return string.Empty;

        var lastSlash = path.TrimEnd('/').LastIndexOf('/');
        return lastSlash <= 0 ? "/" : path[..lastSlash];
    }

    public abstract ValueTask<bool> DirectoryExistsAsync(Uri uri, CancellationToken cancellationToken = default);

    public abstract ValueTask<bool> FileExistsAsync(Uri uri, CancellationToken cancellationToken = default);

    public abstract ValueTask DeleteDirectoryAsync(Uri uri, CancellationToken cancellationToken = default);
    
    public abstract ValueTask DeleteFileAsync(Uri uri, CancellationToken cancellationToken = default);

    public abstract ValueTask<DirectoryProperties> GetDirectoryPropertiesAsync(Uri uri, CancellationToken cancellationToken = default);

    public abstract ValueTask<FileProperties> GetFilePropertiesAsync(Uri uri, CancellationToken cancellationToken = default);

    public abstract ValueTask<Stream> OpenFileStreamAsync(Uri uri, OpenFileMode mode, CancellationToken cancellationToken = default);

    /// <summary>
    /// Opens a file stream with optimized settings for the specified access pattern.
    /// Override this method to provide optimized file I/O for streaming scenarios.
    /// </summary>
    /// <param name="uri">The file URI.</param>
    /// <param name="accessPattern">The expected access pattern (sequential or random).</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>An optimized readable stream.</returns>
    public virtual ValueTask<Stream> OpenOptimizedReadStreamAsync(
        Uri uri,
        Performance.FileAccessPattern accessPattern,
        CancellationToken cancellationToken = default)
    {
        // Default implementation falls back to regular file opening
        return OpenFileStreamAsync(uri, OpenFileMode.Read, cancellationToken);
    }

    public abstract ValueTask CreateDirectoryAsync(Uri uri, CancellationToken cancellationToken);

    public abstract ValueTask<Uri[]> GetFilesAsync(Uri uri, CancellationToken cancellationToken);

    public abstract ValueTask<Uri[]> GetDirectoriesAsync(Uri uri, CancellationToken cancellationToken);

    /// <summary>
    /// Gets the physical file path for a given URI, if available.
    /// Returns null if the store doesn't support physical file access.
    /// When available, enables zero-copy file transfers using OS-level optimizations.
    /// </summary>
    /// <param name="uri">The URI of the file.</param>
    /// <returns>The physical file path, or null if not available.</returns>
    public virtual string? GetPhysicalPath(Uri uri) => null;
}