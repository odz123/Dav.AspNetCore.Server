namespace Dav.AspNetCore.Server.Performance;

/// <summary>
/// Prioritizes initial range requests (first bytes of files) for fast stream starts.
/// NZB and video clients need the file header quickly to begin playback/parsing.
/// </summary>
internal sealed class InitialRangePrioritizer : IDisposable
{
    private static readonly Lazy<InitialRangePrioritizer> LazyInstance = new(() => new InitialRangePrioritizer());
    public static InitialRangePrioritizer Instance => LazyInstance.Value;
    private volatile bool _disposed;

    /// <summary>
    /// Ranges starting within this offset are considered "initial" and get priority.
    /// Files typically have headers in the first 1MB.
    /// </summary>
    public const long InitialRangeThreshold = 1024 * 1024;

    /// <summary>
    /// Maximum concurrent high-priority requests.
    /// </summary>
    private const int MaxConcurrentPriority = 8;

    /// <summary>
    /// Maximum entries to track.
    /// </summary>
    private const int MaxTrackedFiles = 5000;

    private readonly SemaphoreSlim _prioritySemaphore;
    private readonly LruCache<string, FileStartInfo> _fileStarts;

    private InitialRangePrioritizer()
    {
        _prioritySemaphore = new SemaphoreSlim(MaxConcurrentPriority, MaxConcurrentPriority);
        _fileStarts = new LruCache<string, FileStartInfo>(MaxTrackedFiles);
    }

    /// <summary>
    /// Determines if a range request should be prioritized.
    /// Initial ranges get priority to ensure fast stream starts.
    /// </summary>
    /// <param name="offset">The range start offset.</param>
    /// <returns>True if this is a high-priority initial range.</returns>
    public bool IsInitialRange(long offset)
    {
        return offset < InitialRangeThreshold;
    }

    /// <summary>
    /// Gets the priority level for a range request.
    /// Lower numbers = higher priority.
    /// </summary>
    /// <param name="filePath">The file path.</param>
    /// <param name="offset">The range start offset.</param>
    /// <param name="fileSize">The file size.</param>
    /// <returns>Priority level (0 = highest, higher = lower priority).</returns>
    public int GetPriority(string filePath, long offset, long fileSize)
    {
        // Initial range (first 1MB) = highest priority
        if (offset < InitialRangeThreshold)
            return 0;

        // Check if this is a first access to this file
        if (!_fileStarts.TryGetValue(filePath, out var startInfo) || startInfo == null)
        {
            // First access to file - prioritize
            _fileStarts.Set(filePath, new FileStartInfo(offset, DateTime.UtcNow));
            return 1;
        }

        // Sequential access from start = high priority (streaming)
        if (offset < startInfo.MaxSeenOffset + InitialRangeThreshold)
        {
            startInfo.UpdateMaxOffset(offset);
            return 2;
        }

        // Random access / seeking = normal priority
        return 3;
    }

    /// <summary>
    /// Acquires a priority slot for an initial range request.
    /// This ensures initial ranges are processed with priority.
    /// </summary>
    /// <param name="offset">The range offset.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A disposable that releases the slot when done.</returns>
    public async Task<IDisposable?> AcquirePrioritySlotAsync(long offset, CancellationToken cancellationToken)
    {
        if (_disposed || !IsInitialRange(offset))
            return null;

        // Try to acquire without waiting - initial ranges shouldn't block
        if (_prioritySemaphore.Wait(0))
        {
            return new PrioritySlot(_prioritySemaphore);
        }

        // Wait briefly for a slot
        if (await _prioritySemaphore.WaitAsync(TimeSpan.FromMilliseconds(10), cancellationToken))
        {
            return new PrioritySlot(_prioritySemaphore);
        }

        return null;
    }

    /// <summary>
    /// Gets recommended settings for an initial range request.
    /// Optimized for fast TTFB while maintaining good throughput.
    /// </summary>
    /// <param name="offset">The range offset.</param>
    /// <param name="length">The range length.</param>
    /// <returns>Optimized settings for the request.</returns>
    public InitialRangeSettings GetSettings(long offset, long length)
    {
        if (offset < InitialRangeThreshold)
        {
            // Initial range: use optimized buffers based on request size
            // For very small initial requests (< 64KB), use smaller buffers for fastest TTFB
            // For larger initial requests, use larger buffers for better throughput
            var bufferSize = length <= 64 * 1024
                ? 16 * 1024   // 16KB - ultra-fast TTFB for small initial reads
                : length <= 256 * 1024
                    ? 32 * 1024   // 32KB - fast TTFB for medium initial reads
                    : 64 * 1024;  // 64KB - balanced for larger initial reads

            return new InitialRangeSettings(
                bufferSize: bufferSize,
                useAsyncFlush: true,
                disableOutputBuffering: true,
                readAheadSize: Math.Min(length * 2, 2 * 1024 * 1024)  // Prefetch next chunk (up to 2MB)
            );
        }

        // For ranges just past the initial threshold, still use moderate optimizations
        if (offset < InitialRangeThreshold * 2)
        {
            return new InitialRangeSettings(
                bufferSize: 64 * 1024,  // 64KB
                useAsyncFlush: true,
                disableOutputBuffering: true,
                readAheadSize: length  // Prefetch next chunk
            );
        }

        // Normal range - use adaptive buffer size based on throughput
        return new InitialRangeSettings(
            bufferSize: 128 * 1024,  // 128KB for better throughput
            useAsyncFlush: false,
            disableOutputBuffering: false,
            readAheadSize: 0
        );
    }

    /// <summary>
    /// Records that a file's initial range has been served.
    /// </summary>
    public void RecordInitialRangeServed(string filePath, long offset, long length)
    {
        var info = _fileStarts.GetOrAdd(filePath, _ => new FileStartInfo(0, DateTime.UtcNow));
        info.UpdateMaxOffset(offset + length);
        info.MarkInitialServed();
    }

    /// <summary>
    /// Checks if a file's initial range has already been served recently.
    /// </summary>
    public bool HasInitialBeenServed(string filePath)
    {
        if (_fileStarts.TryGetValue(filePath, out var info) && info != null)
        {
            return info.InitialServed && DateTime.UtcNow - info.InitialServedAt < TimeSpan.FromMinutes(5);
        }
        return false;
    }

    private sealed class FileStartInfo
    {
        public long MaxSeenOffset { get; private set; }
        public DateTime FirstAccess { get; }
        public bool InitialServed { get; private set; }
        public DateTime InitialServedAt { get; private set; }
        private readonly object _lock = new();

        public FileStartInfo(long initialOffset, DateTime firstAccess)
        {
            MaxSeenOffset = initialOffset;
            FirstAccess = firstAccess;
        }

        public void UpdateMaxOffset(long offset)
        {
            lock (_lock)
            {
                if (offset > MaxSeenOffset)
                    MaxSeenOffset = offset;
            }
        }

        public void MarkInitialServed()
        {
            lock (_lock)
            {
                if (!InitialServed)
                {
                    InitialServed = true;
                    InitialServedAt = DateTime.UtcNow;
                }
            }
        }
    }

    private sealed class PrioritySlot : IDisposable
    {
        private readonly SemaphoreSlim _semaphore;
        private bool _disposed;

        public PrioritySlot(SemaphoreSlim semaphore)
        {
            _semaphore = semaphore;
        }

        public void Dispose()
        {
            if (_disposed)
                return;

            _disposed = true;
            _semaphore.Release();
        }
    }

    /// <summary>
    /// Disposes the prioritizer and releases associated resources.
    /// </summary>
    public void Dispose()
    {
        if (_disposed)
            return;

        _disposed = true;
        _prioritySemaphore.Dispose();
        _fileStarts.Dispose();
    }
}

/// <summary>
/// Settings optimized for initial range requests.
/// </summary>
public readonly struct InitialRangeSettings
{
    /// <summary>
    /// Buffer size for copying data.
    /// </summary>
    public int BufferSize { get; }

    /// <summary>
    /// Whether to use async flush for headers.
    /// </summary>
    public bool UseAsyncFlush { get; }

    /// <summary>
    /// Whether to disable output buffering.
    /// </summary>
    public bool DisableOutputBuffering { get; }

    /// <summary>
    /// Size of data to read ahead after initial range.
    /// </summary>
    public long ReadAheadSize { get; }

    public InitialRangeSettings(int bufferSize, bool useAsyncFlush, bool disableOutputBuffering, long readAheadSize)
    {
        BufferSize = bufferSize;
        UseAsyncFlush = useAsyncFlush;
        DisableOutputBuffering = disableOutputBuffering;
        ReadAheadSize = readAheadSize;
    }
}
