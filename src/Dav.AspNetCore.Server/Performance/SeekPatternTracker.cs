using System.Collections.Concurrent;

namespace Dav.AspNetCore.Server.Performance;

/// <summary>
/// Tracks seek patterns per file to optimize I/O hints and predict prefetch targets.
/// Detects sequential reads, random access, forward-seeking, and video scrubbing patterns.
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

    /// <summary>
    /// Default prefetch size for video scrubbing patterns (2MB).
    /// </summary>
    public const long DefaultPrefetchSize = 2 * 1024 * 1024;

    /// <summary>
    /// Maximum prefetch size (8MB).
    /// </summary>
    public const long MaxPrefetchSize = 8 * 1024 * 1024;

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
    /// Gets a prefetch hint for the likely next range request.
    /// Returns null if no prediction can be made.
    /// </summary>
    /// <param name="filePath">The file path.</param>
    /// <param name="fileSize">The total file size.</param>
    /// <returns>A prefetch hint with predicted offset and length, or null.</returns>
    public PrefetchHint? GetPrefetchHint(string filePath, long fileSize)
    {
        if (_trackers.TryGetValue(filePath, out var tracker) && tracker != null)
        {
            return tracker.GetPrefetchHint(fileSize);
        }
        return null;
    }

    /// <summary>
    /// Records access and returns both the pattern and a prefetch hint.
    /// </summary>
    public (FileAccessPattern Pattern, PrefetchHint? Prefetch) RecordAccessWithPrefetch(
        string filePath, long offset, long length, long fileSize)
    {
        var tracker = _trackers.GetOrAdd(filePath, _ => new FileAccessTracker());
        var pattern = tracker.RecordAccess(offset, length);
        var prefetch = tracker.GetPrefetchHint(fileSize);
        return (pattern, prefetch);
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

        // For prefetch prediction
        private long _avgSeekDistance;
        private long _avgRequestSize;
        private int _seekSamples;
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
                    _avgSeekDistance = 0;
                    _avgRequestSize = 0;
                    _seekSamples = 0;
                }

                // Analyze the access pattern
                if (_lastAccess != default)
                {
                    var expectedNextOffset = _lastOffset + _lastLength;
                    var seekDistance = offset - expectedNextOffset;

                    // Update moving average of seek distance (for forward seeks)
                    if (seekDistance > 0 && seekDistance <= ForwardSeekThreshold)
                    {
                        _seekSamples++;
                        // Exponential moving average for responsiveness
                        if (_seekSamples == 1)
                        {
                            _avgSeekDistance = seekDistance;
                            _avgRequestSize = length;
                        }
                        else
                        {
                            _avgSeekDistance = (_avgSeekDistance * 3 + seekDistance) / 4;
                            _avgRequestSize = (_avgRequestSize * 3 + length) / 4;
                        }
                    }

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
                else
                {
                    // First access - initialize average request size
                    _avgRequestSize = length;
                }

                _lastOffset = offset;
                _lastLength = length;
                _lastAccess = now;

                return PredictedPattern;
            }
        }

        /// <summary>
        /// Gets a prefetch hint based on observed patterns.
        /// </summary>
        public PrefetchHint? GetPrefetchHint(long fileSize)
        {
            lock (_lock)
            {
                // Need enough samples to make predictions
                if (_seekSamples < 2)
                    return null;

                // Only prefetch for forward-seeking patterns (video scrubbing)
                if (_forwardSeekCount <= _sequentialCount && _forwardSeekCount <= _randomCount)
                    return null;

                // Predict next offset based on pattern
                var predictedNextOffset = _lastOffset + _lastLength + _avgSeekDistance;

                // Don't prefetch past end of file
                if (predictedNextOffset >= fileSize)
                    return null;

                // Prefetch a larger chunk than the average request (read-ahead)
                var prefetchSize = Math.Min(
                    Math.Max(_avgRequestSize * 2, DefaultPrefetchSize),
                    MaxPrefetchSize);

                // Adjust to not exceed file size
                prefetchSize = Math.Min(prefetchSize, fileSize - predictedNextOffset);

                if (prefetchSize <= 0)
                    return null;

                return new PrefetchHint(predictedNextOffset, prefetchSize, _seekSamples);
            }
        }
    }
}

/// <summary>
/// Represents a hint for prefetching data based on access pattern prediction.
/// </summary>
public readonly struct PrefetchHint
{
    /// <summary>
    /// The predicted offset for the next request.
    /// </summary>
    public long PredictedOffset { get; }

    /// <summary>
    /// The recommended prefetch size.
    /// </summary>
    public long PrefetchSize { get; }

    /// <summary>
    /// Confidence level (number of samples used for prediction).
    /// </summary>
    public int Confidence { get; }

    public PrefetchHint(long predictedOffset, long prefetchSize, int confidence)
    {
        PredictedOffset = predictedOffset;
        PrefetchSize = prefetchSize;
        Confidence = confidence;
    }
}
