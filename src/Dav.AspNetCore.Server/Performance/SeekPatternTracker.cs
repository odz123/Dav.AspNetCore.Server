using System.Collections.Concurrent;

namespace Dav.AspNetCore.Server.Performance;

/// <summary>
/// Tracks seek patterns per file to optimize I/O hints.
/// Detects sequential reads, random access, and forward-seeking patterns.
/// </summary>
internal sealed class SeekPatternTracker
{
    private static readonly Lazy<SeekPatternTracker> LazyInstance = new(() => new SeekPatternTracker());
    public static SeekPatternTracker Instance => LazyInstance.Value;

    /// <summary>
    /// Maximum entries to track (LRU eviction when exceeded).
    /// </summary>
    private const int MaxTrackedFiles = 5000;

    /// <summary>
    /// If a seek is within this distance forward, it's considered sequential-like.
    /// </summary>
    private const long ForwardSeekThreshold = 10 * 1024 * 1024; // 10MB

    /// <summary>
    /// Minimum samples before making pattern decisions.
    /// </summary>
    private const int MinSamples = 3;

    /// <summary>
    /// Time window for tracking (older entries expire).
    /// </summary>
    private static readonly TimeSpan ExpirationTime = TimeSpan.FromMinutes(5);

    private readonly LruCache<string, FileAccessTracker> _trackers;

    private SeekPatternTracker()
    {
        _trackers = new LruCache<string, FileAccessTracker>(MaxTrackedFiles);
    }

    /// <summary>
    /// Records a range request and returns the optimal access pattern based on history.
    /// </summary>
    /// <param name="filePath">The file path (used as key).</param>
    /// <param name="offset">The start offset of the range request.</param>
    /// <param name="length">The length of the range request.</param>
    /// <returns>The recommended access pattern based on observed behavior.</returns>
    public FileAccessPattern RecordAccess(string filePath, long offset, long length)
    {
        var tracker = _trackers.GetOrAdd(filePath, _ => new FileAccessTracker());
        return tracker.RecordAccess(offset, length);
    }

    /// <summary>
    /// Gets the predicted access pattern for a file without recording new access.
    /// </summary>
    /// <param name="filePath">The file path.</param>
    /// <returns>The predicted access pattern, or Sequential if unknown.</returns>
    public FileAccessPattern PredictPattern(string filePath)
    {
        if (_trackers.TryGetValue(filePath, out var tracker) && tracker != null)
        {
            return tracker.PredictedPattern;
        }
        return FileAccessPattern.Sequential; // Default to sequential for new files
    }

    /// <summary>
    /// Invalidates tracking data for a file (e.g., when file is modified).
    /// </summary>
    public void Invalidate(string filePath)
    {
        _trackers.TryRemove(filePath, out _);
    }

    /// <summary>
    /// Clears all tracking data.
    /// </summary>
    public void Clear()
    {
        _trackers.Clear();
    }

    /// <summary>
    /// Tracks access patterns for a single file.
    /// </summary>
    private sealed class FileAccessTracker
    {
        private long _lastOffset;
        private long _lastLength;
        private DateTime _lastAccess;
        private int _sequentialCount;
        private int _randomCount;
        private int _forwardSeekCount;
        private readonly object _lock = new();

        public FileAccessPattern PredictedPattern
        {
            get
            {
                lock (_lock)
                {
                    var total = _sequentialCount + _randomCount + _forwardSeekCount;
                    if (total < MinSamples)
                        return FileAccessPattern.Sequential;

                    // If mostly random access, use RandomAccess
                    if (_randomCount > _sequentialCount + _forwardSeekCount)
                        return FileAccessPattern.RandomAccess;

                    // If forward seeking (like video scrubbing), still use Sequential
                    // because the OS read-ahead can help
                    return FileAccessPattern.Sequential;
                }
            }
        }

        public FileAccessPattern RecordAccess(long offset, long length)
        {
            lock (_lock)
            {
                var now = DateTime.UtcNow;

                // Expire old data
                if (now - _lastAccess > ExpirationTime)
                {
                    _sequentialCount = 0;
                    _randomCount = 0;
                    _forwardSeekCount = 0;
                }

                // Analyze the access pattern
                if (_lastAccess != default)
                {
                    var expectedNextOffset = _lastOffset + _lastLength;
                    var seekDistance = offset - expectedNextOffset;

                    if (Math.Abs(seekDistance) < 4096) // Within a page
                    {
                        // Sequential read
                        _sequentialCount++;
                    }
                    else if (seekDistance > 0 && seekDistance <= ForwardSeekThreshold)
                    {
                        // Small forward seek (like skipping)
                        _forwardSeekCount++;
                    }
                    else
                    {
                        // Large backward or forward seek = random
                        _randomCount++;
                    }
                }

                _lastOffset = offset;
                _lastLength = length;
                _lastAccess = now;

                return PredictedPattern;
            }
        }
    }
}
