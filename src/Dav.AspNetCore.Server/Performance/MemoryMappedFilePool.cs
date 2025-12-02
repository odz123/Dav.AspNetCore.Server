using System.IO.MemoryMappedFiles;

namespace Dav.AspNetCore.Server.Performance;

/// <summary>
/// Provides pooled memory-mapped file access for fast random reads.
/// Memory-mapped files provide the fastest possible seeking because the OS
/// handles page faults and caching at the kernel level.
/// </summary>
internal sealed class MemoryMappedFilePool : IDisposable
{
    private static readonly Lazy<MemoryMappedFilePool> LazyInstance = new(() => new MemoryMappedFilePool());
    public static MemoryMappedFilePool Instance => LazyInstance.Value;

    /// <summary>
    /// Files must be at least this size to use memory mapping (256KB).
    /// Smaller files don't benefit from memory mapping overhead.
    /// </summary>
    public const long MinimumFileSize = 256 * 1024;

    /// <summary>
    /// Maximum size for memory mapping (2GB on 32-bit, 16GB on 64-bit).
    /// Larger files should use regular streaming.
    /// </summary>
    public static readonly long MaximumFileSize = Environment.Is64BitProcess
        ? 16L * 1024 * 1024 * 1024
        : 2L * 1024 * 1024 * 1024;

    /// <summary>
    /// Maximum entries to keep in the pool.
    /// </summary>
    private const int MaxPooledEntries = 100;

    /// <summary>
    /// How long to keep an unused entry before disposing.
    /// </summary>
    private static readonly TimeSpan EntryExpiration = TimeSpan.FromMinutes(5);

    private readonly LruCache<string, PooledMappedFile> _pool;
    private readonly Timer _cleanupTimer;
    private bool _disposed;

    private MemoryMappedFilePool()
    {
        _pool = new LruCache<string, PooledMappedFile>(MaxPooledEntries);
        // Run cleanup every minute
        _cleanupTimer = new Timer(CleanupCallback, null, TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(1));
    }

    /// <summary>
    /// Determines if a file should use memory mapping based on size and access pattern.
    /// </summary>
    public static bool ShouldUseMemoryMapping(long fileSize, FileAccessPattern accessPattern)
    {
        // Only use for random access patterns (seeking)
        if (accessPattern != FileAccessPattern.RandomAccess)
            return false;

        // Check size bounds
        return fileSize >= MinimumFileSize && fileSize <= MaximumFileSize;
    }

    /// <summary>
    /// Gets or creates a memory-mapped file for fast random access.
    /// </summary>
    /// <param name="filePath">The physical file path.</param>
    /// <param name="fileSize">The file size.</param>
    /// <param name="lastModified">The file's last modification time for cache validation.</param>
    /// <returns>A stream that reads from the memory-mapped file, or null if mapping failed.</returns>
    public Stream? GetMappedStream(string filePath, long fileSize, DateTime lastModified)
    {
        if (_disposed)
            return null;

        try
        {
            var cacheKey = filePath;

            // Try to get from pool
            if (_pool.TryGetValue(cacheKey, out var pooledFile) && pooledFile != null)
            {
                // Validate the cached entry
                if (pooledFile.FileSize == fileSize && pooledFile.LastModified == lastModified)
                {
                    pooledFile.LastAccess = DateTime.UtcNow;
                    return pooledFile.CreateViewStream();
                }

                // File changed, dispose old entry
                _pool.TryRemove(cacheKey, out _);
                pooledFile.Dispose();
            }

            // Create new memory-mapped file
            var mappedFile = MemoryMappedFile.CreateFromFile(
                filePath,
                FileMode.Open,
                mapName: null,
                capacity: 0,
                MemoryMappedFileAccess.Read);

            pooledFile = new PooledMappedFile(mappedFile, fileSize, lastModified);
            _pool.Set(cacheKey, pooledFile);

            return pooledFile.CreateViewStream();
        }
        catch
        {
            // Memory mapping can fail for various reasons (access denied, etc.)
            // Fall back to regular stream
            return null;
        }
    }

    /// <summary>
    /// Creates a view stream for a specific range of a memory-mapped file.
    /// This is extremely fast as no actual I/O occurs until the data is accessed.
    /// </summary>
    public Stream? GetMappedRangeStream(string filePath, long fileSize, DateTime lastModified, long offset, long length)
    {
        if (_disposed)
            return null;

        try
        {
            var cacheKey = filePath;

            // Try to get from pool
            if (_pool.TryGetValue(cacheKey, out var pooledFile) && pooledFile != null)
            {
                if (pooledFile.FileSize == fileSize && pooledFile.LastModified == lastModified)
                {
                    pooledFile.LastAccess = DateTime.UtcNow;
                    return pooledFile.CreateViewStream(offset, length);
                }

                _pool.TryRemove(cacheKey, out _);
                pooledFile.Dispose();
            }

            // Create new memory-mapped file
            var mappedFile = MemoryMappedFile.CreateFromFile(
                filePath,
                FileMode.Open,
                mapName: null,
                capacity: 0,
                MemoryMappedFileAccess.Read);

            pooledFile = new PooledMappedFile(mappedFile, fileSize, lastModified);
            _pool.Set(cacheKey, pooledFile);

            return pooledFile.CreateViewStream(offset, length);
        }
        catch
        {
            return null;
        }
    }

    /// <summary>
    /// Invalidates a cached memory-mapped file (e.g., when file is modified).
    /// </summary>
    public void Invalidate(string filePath)
    {
        if (_pool.TryRemove(filePath, out var pooledFile) && pooledFile != null)
        {
            pooledFile.Dispose();
        }
    }

    private void CleanupCallback(object? state)
    {
        if (_disposed)
            return;

        var expiredKeys = new List<string>();
        var now = DateTime.UtcNow;

        foreach (var key in _pool.Keys)
        {
            if (_pool.TryGetValue(key, out var entry) && entry != null)
            {
                if (now - entry.LastAccess > EntryExpiration)
                {
                    expiredKeys.Add(key);
                }
            }
        }

        foreach (var key in expiredKeys)
        {
            if (_pool.TryRemove(key, out var entry) && entry != null)
            {
                entry.Dispose();
            }
        }
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        _disposed = true;
        _cleanupTimer.Dispose();

        foreach (var key in _pool.Keys)
        {
            if (_pool.TryRemove(key, out var entry) && entry != null)
            {
                entry.Dispose();
            }
        }
    }

    /// <summary>
    /// Wraps a memory-mapped file with metadata for cache validation.
    /// </summary>
    private sealed class PooledMappedFile : IDisposable
    {
        private readonly MemoryMappedFile _mappedFile;
        private readonly object _lock = new();
        public long FileSize { get; }
        public DateTime LastModified { get; }
        public DateTime LastAccess { get; set; }
        private bool _disposed;

        public PooledMappedFile(MemoryMappedFile mappedFile, long fileSize, DateTime lastModified)
        {
            _mappedFile = mappedFile;
            FileSize = fileSize;
            LastModified = lastModified;
            LastAccess = DateTime.UtcNow;
        }

        public Stream? CreateViewStream()
        {
            lock (_lock)
            {
                if (_disposed)
                    return null;

                try
                {
                    return _mappedFile.CreateViewStream(0, FileSize, MemoryMappedFileAccess.Read);
                }
                catch (ObjectDisposedException)
                {
                    return null;
                }
            }
        }

        public Stream? CreateViewStream(long offset, long length)
        {
            lock (_lock)
            {
                if (_disposed)
                    return null;

                // Ensure we don't exceed file bounds
                var safeLength = Math.Min(length, FileSize - offset);
                if (safeLength <= 0)
                    return Stream.Null;

                try
                {
                    return _mappedFile.CreateViewStream(offset, safeLength, MemoryMappedFileAccess.Read);
                }
                catch (ObjectDisposedException)
                {
                    return null;
                }
            }
        }

        public void Dispose()
        {
            lock (_lock)
            {
                if (_disposed)
                    return;

                _disposed = true;
            }

            // Dispose outside the lock to avoid potential deadlocks
            try
            {
                _mappedFile.Dispose();
            }
            catch
            {
                // Ignore disposal errors
            }
        }
    }
}
