using System.Collections.Concurrent;

namespace Dav.AspNetCore.Server.Performance;

/// <summary>
/// Optimizes seek operations for NZB streaming by predicting and pre-caching
/// commonly accessed positions within files.
///
/// Key features:
/// - Predicts next seek positions based on access patterns
/// - Pre-warms kernel page cache for predicted positions
/// - Tracks common seek positions per file (e.g., chapter markers in video)
/// - Coordinates with memory-mapped file pool for instant seeks
/// </summary>
internal sealed class SeekOptimizer
{
    private static readonly Lazy<SeekOptimizer> LazyInstance = new(() => new SeekOptimizer());
    public static SeekOptimizer Instance => LazyInstance.Value;

    /// <summary>
    /// Maximum files to track.
    /// </summary>
    private const int MaxTrackedFiles = 500;

    /// <summary>
    /// Maximum seek positions to track per file.
    /// </summary>
    private const int MaxPositionsPerFile = 100;

    /// <summary>
    /// Amount of data to pre-cache at each predicted position.
    /// </summary>
    private const long PreCacheSize = 2 * 1024 * 1024; // 2MB

    /// <summary>
    /// Confidence threshold for triggering pre-cache (0-100).
    /// </summary>
    private const int PreCacheConfidenceThreshold = 60;

    private readonly LruCache<string, FileSeekProfile> _fileProfiles;
    private readonly ConcurrentDictionary<string, DateTime> _lastPrefetchTimes;
    private readonly Timer _cleanupTimer;

    private SeekOptimizer()
    {
        _fileProfiles = new LruCache<string, FileSeekProfile>(MaxTrackedFiles);
        _lastPrefetchTimes = new ConcurrentDictionary<string, DateTime>();
        _cleanupTimer = new Timer(CleanupCallback, null, TimeSpan.FromMinutes(5), TimeSpan.FromMinutes(5));
    }

    /// <summary>
    /// Records a seek operation for pattern learning.
    /// </summary>
    /// <param name="physicalPath">The file path.</param>
    /// <param name="offset">The seek offset.</param>
    /// <param name="length">The read length.</param>
    /// <param name="fileSize">The total file size.</param>
    public void RecordSeek(string physicalPath, long offset, long length, long fileSize)
    {
        if (string.IsNullOrEmpty(physicalPath))
            return;

        var profile = _fileProfiles.GetOrAdd(physicalPath, _ => new FileSeekProfile(fileSize));
        profile.RecordAccess(offset, length);

        // Check if we should trigger predictive prefetch
        var prediction = profile.PredictNext(offset, length);
        if (prediction.Confidence >= PreCacheConfidenceThreshold)
        {
            TriggerPredictivePrefetch(physicalPath, prediction, fileSize);
        }
    }

    /// <summary>
    /// Gets predictions for likely next seek positions.
    /// </summary>
    /// <param name="physicalPath">The file path.</param>
    /// <param name="currentOffset">Current position.</param>
    /// <param name="currentLength">Current read length.</param>
    /// <returns>List of predicted positions with confidence scores.</returns>
    public IReadOnlyList<SeekPredictionResult> GetPredictions(
        string physicalPath,
        long currentOffset,
        long currentLength)
    {
        if (!_fileProfiles.TryGetValue(physicalPath, out var profile) || profile == null)
            return Array.Empty<SeekPredictionResult>();

        return profile.GetTopPredictions(currentOffset, currentLength, 5);
    }

    /// <summary>
    /// Pre-warms the cache for predicted seek positions.
    /// Call this when a file is first accessed to prime the cache.
    /// </summary>
    /// <param name="physicalPath">The file path.</param>
    /// <param name="fileSize">The file size.</param>
    public void PreWarmCommonPositions(string physicalPath, long fileSize)
    {
        if (string.IsNullOrEmpty(physicalPath) || fileSize <= 0)
            return;

        // Don't prewarm too frequently
        var cacheKey = $"prewarm:{physicalPath}";
        if (_lastPrefetchTimes.TryGetValue(cacheKey, out var lastTime))
        {
            if (DateTime.UtcNow - lastTime < TimeSpan.FromMinutes(1))
                return;
        }
        _lastPrefetchTimes[cacheKey] = DateTime.UtcNow;

        // Get common positions from profile
        if (_fileProfiles.TryGetValue(physicalPath, out var profile) && profile != null)
        {
            var commonPositions = profile.GetMostAccessedPositions(5);
            foreach (var pos in commonPositions)
            {
                PrefetchPosition(physicalPath, pos, Math.Min(PreCacheSize, fileSize - pos));
            }
        }
        else
        {
            // For new files, prefetch common positions:
            // - Start (headers)
            // - 25%, 50%, 75% marks (common video seek points)
            var positions = new[]
            {
                0L,
                fileSize / 4,
                fileSize / 2,
                (fileSize * 3) / 4
            };

            foreach (var pos in positions.Where(p => p < fileSize))
            {
                PrefetchPosition(physicalPath, pos, Math.Min(PreCacheSize, fileSize - pos));
            }
        }
    }

    /// <summary>
    /// Records that a file's metadata indicates common seek positions
    /// (e.g., chapter markers, keyframes).
    /// </summary>
    public void RecordKnownSeekPoints(string physicalPath, long fileSize, IEnumerable<long> positions)
    {
        if (string.IsNullOrEmpty(physicalPath))
            return;

        var profile = _fileProfiles.GetOrAdd(physicalPath, _ => new FileSeekProfile(fileSize));
        profile.AddKnownSeekPoints(positions);
    }

    /// <summary>
    /// Checks if a position is likely to be accessed soon.
    /// Use this to decide whether to keep data in cache.
    /// </summary>
    public bool IsLikelyToBeAccessed(string physicalPath, long offset, long fileSize)
    {
        if (!_fileProfiles.TryGetValue(physicalPath, out var profile) || profile == null)
            return false;

        return profile.IsHotPosition(offset, fileSize);
    }

    /// <summary>
    /// Optimizes a seek operation by applying kernel hints and triggering prefetch.
    /// </summary>
    public void OptimizeSeek(string physicalPath, long offset, long length, long fileSize)
    {
        // Record the seek
        RecordSeek(physicalPath, offset, length, fileSize);

        // Apply kernel hints for the current read
        if (LinuxKernelHints.IsAvailable)
        {
            LinuxKernelHints.PrefetchFileRange(physicalPath, offset, length);
        }

        // Get predictions and prefetch
        var predictions = GetPredictions(physicalPath, offset, length);
        foreach (var pred in predictions.Where(p => p.Confidence >= PreCacheConfidenceThreshold))
        {
            var prefetchLength = Math.Min(PreCacheSize, fileSize - pred.Offset);
            if (prefetchLength > 0)
            {
                PrefetchPosition(physicalPath, pred.Offset, prefetchLength);
            }
        }
    }

    /// <summary>
    /// Gets the optimal buffer size for a seek based on pattern.
    /// </summary>
    public int GetOptimalBufferForSeek(string physicalPath, long offset, long requestedLength)
    {
        if (!_fileProfiles.TryGetValue(physicalPath, out var profile) || profile == null)
            return (int)Math.Min(requestedLength, 128 * 1024);

        var pattern = profile.DetectPattern();

        return pattern switch
        {
            SeekAccessPattern.Sequential => (int)Math.Min(requestedLength, 256 * 1024),
            SeekAccessPattern.ForwardScrubbing => (int)Math.Min(requestedLength, 128 * 1024),
            SeekAccessPattern.BackwardScrubbing => (int)Math.Min(requestedLength, 64 * 1024),
            SeekAccessPattern.ChapterJumping => (int)Math.Min(requestedLength, 64 * 1024),
            _ => (int)Math.Min(requestedLength, 128 * 1024)
        };
    }

    private void TriggerPredictivePrefetch(string physicalPath, PredictionResult prediction, long fileSize)
    {
        // Rate limit prefetch operations
        var cacheKey = $"pred:{physicalPath}:{prediction.Offset / (1024 * 1024)}";
        if (_lastPrefetchTimes.TryGetValue(cacheKey, out var lastTime))
        {
            if (DateTime.UtcNow - lastTime < TimeSpan.FromMilliseconds(100))
                return;
        }
        _lastPrefetchTimes[cacheKey] = DateTime.UtcNow;

        var prefetchLength = Math.Min(PreCacheSize, fileSize - prediction.Offset);
        if (prefetchLength > 0)
        {
            PrefetchPosition(physicalPath, prediction.Offset, prefetchLength);
        }
    }

    private static void PrefetchPosition(string physicalPath, long offset, long length)
    {
        if (LinuxKernelHints.IsAvailable)
        {
            LinuxKernelHints.PrefetchFileRange(physicalPath, offset, length);
        }

        // Also queue through PrefetchService for deeper prefetch
        PrefetchService.Instance.QueuePrefetch(physicalPath, offset, length);
    }

    private void CleanupCallback(object? state)
    {
        try
        {
            var cutoff = DateTime.UtcNow - TimeSpan.FromMinutes(10);
            var keysToRemove = _lastPrefetchTimes
                .Where(kvp => kvp.Value < cutoff)
                .Select(kvp => kvp.Key)
                .ToList();

            foreach (var key in keysToRemove)
            {
                _lastPrefetchTimes.TryRemove(key, out _);
            }
        }
        catch
        {
            // Non-critical cleanup
        }
    }

    /// <summary>
    /// Tracks seek patterns for a single file.
    /// </summary>
    private sealed class FileSeekProfile
    {
        private readonly long _fileSize;
        private readonly List<SeekRecord> _history = new();
        private readonly Dictionary<long, int> _positionCounts = new();
        private readonly HashSet<long> _knownSeekPoints = new();
        private readonly object _lock = new();

        public FileSeekProfile(long fileSize)
        {
            _fileSize = fileSize;
        }

        public void RecordAccess(long offset, long length)
        {
            lock (_lock)
            {
                _history.Add(new SeekRecord(offset, length, DateTime.UtcNow));

                // Keep history bounded
                while (_history.Count > 1000)
                    _history.RemoveAt(0);

                // Track position access counts (bucket by 1MB)
                var bucket = offset / (1024 * 1024);
                _positionCounts.TryGetValue(bucket, out var count);
                _positionCounts[bucket] = count + 1;
            }
        }

        public void AddKnownSeekPoints(IEnumerable<long> positions)
        {
            lock (_lock)
            {
                foreach (var pos in positions.Where(p => p >= 0 && p < _fileSize))
                {
                    _knownSeekPoints.Add(pos);
                }
            }
        }

        public PredictionResult PredictNext(long currentOffset, long currentLength)
        {
            lock (_lock)
            {
                if (_history.Count < 2)
                    return new PredictionResult(currentOffset + currentLength, 30);

                // Analyze recent history
                var recent = _history.TakeLast(10).ToList();

                // Check for sequential pattern
                var isSequential = true;
                for (int i = 1; i < recent.Count; i++)
                {
                    var expected = recent[i - 1].Offset + recent[i - 1].Length;
                    if (Math.Abs(recent[i].Offset - expected) > 64 * 1024)
                    {
                        isSequential = false;
                        break;
                    }
                }

                if (isSequential)
                {
                    return new PredictionResult(currentOffset + currentLength, 90);
                }

                // Check for stride pattern
                var strides = new List<long>();
                for (int i = 1; i < recent.Count; i++)
                {
                    strides.Add(recent[i].Offset - recent[i - 1].Offset);
                }

                if (strides.Count >= 3)
                {
                    var avgStride = strides.Average();
                    var variance = strides.Select(s => Math.Pow(s - avgStride, 2)).Average();
                    var stdDev = Math.Sqrt(variance);

                    // Low variance means consistent stride
                    if (stdDev < Math.Abs(avgStride) * 0.3)
                    {
                        var predictedOffset = currentOffset + (long)avgStride;
                        if (predictedOffset >= 0 && predictedOffset < _fileSize)
                        {
                            return new PredictionResult(predictedOffset, 75);
                        }
                    }
                }

                // Check if heading toward a known seek point
                var nearestSeekPoint = _knownSeekPoints
                    .Where(p => p > currentOffset)
                    .OrderBy(p => p)
                    .FirstOrDefault();

                if (nearestSeekPoint > 0)
                {
                    return new PredictionResult(nearestSeekPoint, 60);
                }

                // Default: predict continuation
                return new PredictionResult(currentOffset + currentLength, 40);
            }
        }

        public IReadOnlyList<SeekPredictionResult> GetTopPredictions(long currentOffset, long currentLength, int count)
        {
            lock (_lock)
            {
                var predictions = new List<SeekPredictionResult>();

                // Add sequential prediction
                predictions.Add(new SeekPredictionResult(currentOffset + currentLength, 80));

                // Add most accessed positions
                var topPositions = _positionCounts
                    .OrderByDescending(kvp => kvp.Value)
                    .Take(count)
                    .Select(kvp => kvp.Key * 1024 * 1024);

                foreach (var pos in topPositions.Where(p => p != currentOffset && p < _fileSize))
                {
                    var accessCount = _positionCounts[(int)(pos / (1024 * 1024))];
                    var confidence = Math.Min(95, 40 + accessCount * 10);
                    predictions.Add(new SeekPredictionResult(pos, confidence));
                }

                // Add known seek points
                foreach (var seekPoint in _knownSeekPoints.Where(p => p > currentOffset).Take(3))
                {
                    predictions.Add(new SeekPredictionResult(seekPoint, 70));
                }

                return predictions
                    .OrderByDescending(p => p.Confidence)
                    .DistinctBy(p => p.Offset / (1024 * 1024)) // Dedupe by 1MB bucket
                    .Take(count)
                    .ToList();
            }
        }

        public IReadOnlyList<long> GetMostAccessedPositions(int count)
        {
            lock (_lock)
            {
                return _positionCounts
                    .OrderByDescending(kvp => kvp.Value)
                    .Take(count)
                    .Select(kvp => kvp.Key * 1024 * 1024)
                    .Where(p => p < _fileSize)
                    .ToList();
            }
        }

        public bool IsHotPosition(long offset, long fileSize)
        {
            lock (_lock)
            {
                var bucket = offset / (1024 * 1024);
                if (_positionCounts.TryGetValue(bucket, out var count))
                {
                    return count >= 3;
                }

                // Check if near a known seek point
                return _knownSeekPoints.Any(p => Math.Abs(p - offset) < 1024 * 1024);
            }
        }

        public SeekAccessPattern DetectPattern()
        {
            lock (_lock)
            {
                if (_history.Count < 5)
                    return SeekAccessPattern.Unknown;

                var recent = _history.TakeLast(10).ToList();

                // Calculate strides
                var strides = new List<long>();
                for (int i = 1; i < recent.Count; i++)
                {
                    strides.Add(recent[i].Offset - recent[i - 1].Offset);
                }

                var avgStride = strides.Average();
                var positiveStrides = strides.Count(s => s > 0);
                var negativeStrides = strides.Count(s => s < 0);
                var smallStrides = strides.Count(s => Math.Abs(s) < 100 * 1024);

                // Sequential: small positive strides
                if (smallStrides >= strides.Count * 0.7 && positiveStrides >= strides.Count * 0.8)
                    return SeekAccessPattern.Sequential;

                // Forward scrubbing: larger forward jumps
                if (positiveStrides >= strides.Count * 0.7 && avgStride > 1024 * 1024)
                    return SeekAccessPattern.ForwardScrubbing;

                // Backward scrubbing
                if (negativeStrides >= strides.Count * 0.5)
                    return SeekAccessPattern.BackwardScrubbing;

                // Chapter jumping: large jumps to consistent positions
                var uniqueBuckets = recent.Select(r => r.Offset / (10 * 1024 * 1024)).Distinct().Count();
                if (uniqueBuckets <= recent.Count / 2 && Math.Abs(avgStride) > 10 * 1024 * 1024)
                    return SeekAccessPattern.ChapterJumping;

                return SeekAccessPattern.Random;
            }
        }

        private readonly record struct SeekRecord(long Offset, long Length, DateTime Time);
    }

    private readonly record struct PredictionResult(long Offset, int Confidence);
}

/// <summary>
/// A seek prediction with confidence score.
/// </summary>
public readonly struct SeekPredictionResult
{
    /// <summary>Predicted seek offset.</summary>
    public long Offset { get; }

    /// <summary>Confidence score (0-100).</summary>
    public int Confidence { get; }

    public SeekPredictionResult(long offset, int confidence)
    {
        Offset = offset;
        Confidence = Math.Clamp(confidence, 0, 100);
    }
}

/// <summary>
/// Detected seek access patterns.
/// </summary>
public enum SeekAccessPattern
{
    /// <summary>Unknown or insufficient data.</summary>
    Unknown,

    /// <summary>Sequential forward access.</summary>
    Sequential,

    /// <summary>Forward scrubbing (video timeline).</summary>
    ForwardScrubbing,

    /// <summary>Backward scrubbing.</summary>
    BackwardScrubbing,

    /// <summary>Jumping between chapters/markers.</summary>
    ChapterJumping,

    /// <summary>Random access pattern.</summary>
    Random
}
