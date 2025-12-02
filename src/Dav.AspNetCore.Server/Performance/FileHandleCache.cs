using System.Collections.Concurrent;
using Microsoft.Win32.SafeHandles;

namespace Dav.AspNetCore.Server.Performance;

/// <summary>
/// Caches open file handles for frequently accessed files.
/// Keeping file handles open eliminates the overhead of open/close syscalls
/// and allows kernel optimizations like page cache affinity.
/// </summary>
internal sealed class FileHandleCache : IDisposable
{
    private static readonly Lazy<FileHandleCache> LazyInstance = new(() => new FileHandleCache());
    public static FileHandleCache Instance => LazyInstance.Value;

    /// <summary>
    /// Maximum number of file handles to cache.
    /// </summary>
    private const int MaxCachedHandles = 200;

    /// <summary>
    /// How long to keep an unused handle before closing.
    /// </summary>
    private static readonly TimeSpan HandleExpiration = TimeSpan.FromMinutes(5);

    /// <summary>
    /// Minimum access count before caching a file handle.
    /// This prevents caching one-off requests.
    /// </summary>
    private const int MinAccessCountForCaching = 2;

    private readonly LruCache<string, CachedHandle> _handleCache;
    private readonly ConcurrentDictionary<string, FileAccessStats> _accessStats;
    private readonly Timer _cleanupTimer;
    private volatile bool _disposed;
    private int _cleanupRunning;

    private FileHandleCache()
    {
        _handleCache = new LruCache<string, CachedHandle>(MaxCachedHandles);
        _accessStats = new ConcurrentDictionary<string, FileAccessStats>();

        // Cleanup every 30 seconds
        _cleanupTimer = new Timer(CleanupCallback, null,
            TimeSpan.FromSeconds(30), TimeSpan.FromSeconds(30));
    }

    /// <summary>
    /// Gets or opens a file handle for reading.
    /// Returns a cached handle if available, or opens a new one.
    /// </summary>
    /// <param name="filePath">The file path.</param>
    /// <param name="accessPattern">The expected access pattern.</param>
    /// <returns>A file handle wrapper, or null if the file cannot be opened.</returns>
    public CachedFileHandle? GetHandle(string filePath, FileAccessPattern accessPattern)
    {
        if (_disposed || string.IsNullOrEmpty(filePath))
            return null;

        // Track access for caching decisions
        var stats = _accessStats.GetOrAdd(filePath, _ => new FileAccessStats());
        stats.RecordAccess();

        // Try to get existing cached handle
        if (_handleCache.TryGetValue(filePath, out var cachedHandle) && cachedHandle != null)
        {
            if (cachedHandle.IsValid)
            {
                cachedHandle.RecordUse();
                return new CachedFileHandle(cachedHandle, isOwned: false);
            }
            else
            {
                // Handle is invalid, remove it
                _handleCache.TryRemove(filePath, out _);
                cachedHandle.Dispose();
            }
        }

        // Open new handle
        try
        {
            var fileOptions = GetFileOptions(accessPattern);
            var fileStream = new FileStream(
                filePath,
                FileMode.Open,
                FileAccess.Read,
                FileShare.Read,
                bufferSize: 0, // No buffering - we manage our own
                fileOptions);

            var newHandle = new CachedHandle(filePath, fileStream);

            // Decide whether to cache this handle
            bool shouldCache = stats.AccessCount >= MinAccessCountForCaching;

            if (shouldCache && _handleCache.Count < MaxCachedHandles)
            {
                // Apply kernel hints for cached handles
                if (LinuxKernelHints.IsAvailable)
                {
                    var fd = LinuxKernelHints.GetFileDescriptor(fileStream);
                    if (fd >= 0)
                    {
                        // Set appropriate access pattern hint
                        if (accessPattern == FileAccessPattern.Sequential)
                        {
                            LinuxKernelHints.AdviseSequential(fd);
                        }
                        else
                        {
                            LinuxKernelHints.AdviseRandom(fd);
                        }
                    }
                }

                _handleCache.Set(filePath, newHandle);
                return new CachedFileHandle(newHandle, isOwned: false);
            }
            else
            {
                // Not caching - caller owns the handle
                return new CachedFileHandle(newHandle, isOwned: true);
            }
        }
        catch
        {
            return null;
        }
    }

    /// <summary>
    /// Invalidates a cached handle (e.g., when the file is modified).
    /// </summary>
    public void Invalidate(string filePath)
    {
        if (_handleCache.TryRemove(filePath, out var handle) && handle != null)
        {
            handle.Dispose();
        }
        _accessStats.TryRemove(filePath, out _);
    }

    /// <summary>
    /// Pre-opens a file handle for a known hot file.
    /// Use this when you know a file will be accessed frequently.
    /// </summary>
    public void PreOpen(string filePath, FileAccessPattern accessPattern)
    {
        if (_disposed || string.IsNullOrEmpty(filePath))
            return;

        // Record accesses to bypass the minimum access check
        var stats = _accessStats.GetOrAdd(filePath, _ => new FileAccessStats());
        stats.RecordAccess();
        stats.RecordAccess();

        // Force cache population
        var handle = GetHandle(filePath, accessPattern);
        handle?.Dispose(); // This won't close the cached handle
    }

    private static FileOptions GetFileOptions(FileAccessPattern pattern)
    {
        var options = FileOptions.Asynchronous;

        if (pattern == FileAccessPattern.Sequential)
        {
            options |= FileOptions.SequentialScan;
        }
        else
        {
            options |= FileOptions.RandomAccess;
        }

        return options;
    }

    private void CleanupCallback(object? state)
    {
        if (_disposed)
            return;

        // Prevent concurrent cleanup runs
        if (Interlocked.CompareExchange(ref _cleanupRunning, 1, 0) != 0)
            return;

        try
        {
            var now = DateTime.UtcNow;
            var keysToRemove = new List<string>();

            // Find expired handles
            foreach (var key in _handleCache.Keys)
            {
                if (_disposed) break; // Check for disposal during iteration

                if (_handleCache.TryGetValue(key, out var handle) && handle != null)
                {
                    if (!handle.IsValid || (now - handle.LastUsed) > HandleExpiration)
                    {
                        keysToRemove.Add(key);
                    }
                }
            }

            // Remove expired handles
            foreach (var key in keysToRemove)
            {
                if (_disposed) break; // Check for disposal during iteration

                if (_handleCache.TryRemove(key, out var handle) && handle != null)
                {
                    handle.Dispose();
                }
            }

            // Clean up old access stats
            var statsToRemove = new List<string>();
            foreach (var kvp in _accessStats)
            {
                if ((now - kvp.Value.LastAccess) > TimeSpan.FromMinutes(10))
                {
                    statsToRemove.Add(kvp.Key);
                }
            }
            foreach (var key in statsToRemove)
            {
                _accessStats.TryRemove(key, out _);
            }
        }
        catch
        {
            // Cleanup errors are non-critical
        }
        finally
        {
            Interlocked.Exchange(ref _cleanupRunning, 0);
        }
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        _disposed = true;
        _cleanupTimer.Dispose();

        // Wait for any running cleanup to finish
        SpinWait.SpinUntil(() => Interlocked.CompareExchange(ref _cleanupRunning, 0, 0) == 0, TimeSpan.FromSeconds(5));

        foreach (var key in _handleCache.Keys.ToList())
        {
            if (_handleCache.TryRemove(key, out var handle) && handle != null)
            {
                handle.Dispose();
            }
        }

        _handleCache.Dispose();
    }

    /// <summary>
    /// Tracks access patterns for caching decisions.
    /// </summary>
    private sealed class FileAccessStats
    {
        private int _accessCount;
        private DateTime _lastAccess;
        private readonly object _lock = new();

        public int AccessCount
        {
            get
            {
                lock (_lock)
                    return _accessCount;
            }
        }

        public DateTime LastAccess
        {
            get
            {
                lock (_lock)
                    return _lastAccess;
            }
        }

        public void RecordAccess()
        {
            lock (_lock)
            {
                _accessCount++;
                _lastAccess = DateTime.UtcNow;
            }
        }
    }

    /// <summary>
    /// A cached file handle with metadata.
    /// </summary>
    internal sealed class CachedHandle : IDisposable
    {
        public string FilePath { get; }
        public FileStream Stream { get; }
        public DateTime LastUsed { get; private set; }
        public int UseCount { get; private set; }
        private bool _disposed;
        private readonly object _lock = new();

        public CachedHandle(string filePath, FileStream stream)
        {
            FilePath = filePath;
            Stream = stream;
            LastUsed = DateTime.UtcNow;
        }

        public bool IsValid
        {
            get
            {
                lock (_lock)
                {
                    return !_disposed &&
                           Stream.SafeFileHandle != null &&
                           !Stream.SafeFileHandle.IsInvalid &&
                           !Stream.SafeFileHandle.IsClosed;
                }
            }
        }

        public void RecordUse()
        {
            lock (_lock)
            {
                UseCount++;
                LastUsed = DateTime.UtcNow;
            }
        }

        public void Dispose()
        {
            lock (_lock)
            {
                if (_disposed)
                    return;

                _disposed = true;
                try
                {
                    Stream.Dispose();
                }
                catch
                {
                    // Ignore dispose errors
                }
            }
        }
    }
}

/// <summary>
/// A wrapper for a cached file handle that manages ownership.
/// </summary>
internal sealed class CachedFileHandle : IDisposable
{
    private readonly FileHandleCache.CachedHandle _handle;
    private readonly bool _isOwned;
    private bool _disposed;

    public CachedFileHandle(FileHandleCache.CachedHandle handle, bool isOwned)
    {
        _handle = handle;
        _isOwned = isOwned;
    }

    /// <summary>
    /// Gets the underlying file stream.
    /// </summary>
    public FileStream Stream => _handle.Stream;

    /// <summary>
    /// Gets the file descriptor for use with kernel hints.
    /// </summary>
    public int FileDescriptor => LinuxKernelHints.GetFileDescriptor(_handle.Stream);

    /// <summary>
    /// Creates a new stream positioned at the specified offset.
    /// </summary>
    public FileStream CreatePositionedStream(long offset)
    {
        var stream = _handle.Stream;
        stream.Seek(offset, SeekOrigin.Begin);
        return stream;
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        _disposed = true;

        // Only dispose if we own the handle (not cached)
        if (_isOwned)
        {
            _handle.Dispose();
        }
    }
}
