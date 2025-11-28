using System.Diagnostics;

namespace Dav.AspNetCore.Server.Performance;

/// <summary>
/// Provides adaptive prefetch sizing based on observed throughput and latency.
/// Automatically adjusts prefetch sizes to optimize for the current I/O characteristics.
/// </summary>
internal sealed class AdaptivePrefetch
{
    private static readonly Lazy<AdaptivePrefetch> LazyInstance = new(() => new AdaptivePrefetch());
    public static AdaptivePrefetch Instance => LazyInstance.Value;

    /// <summary>
    /// Minimum prefetch size (64KB).
    /// </summary>
    public const long MinPrefetchSize = 64 * 1024;

    /// <summary>
    /// Maximum prefetch size (16MB).
    /// </summary>
    public const long MaxPrefetchSize = 16 * 1024 * 1024;

    /// <summary>
    /// Target latency for prefetch operations (100ms).
    /// We aim to keep prefetch within this time budget.
    /// </summary>
    private const long TargetLatencyMs = 100;

    /// <summary>
    /// Maximum entries to track.
    /// </summary>
    private const int MaxTrackedFiles = 2000;

    private readonly LruCache<string, ThroughputTracker> _trackers;

    // Global throughput estimate (starts conservative)
    private long _globalThroughputBytesPerSecond = 50 * 1024 * 1024; // 50 MB/s initial estimate
    private readonly object _globalLock = new();

    private AdaptivePrefetch()
    {
        _trackers = new LruCache<string, ThroughputTracker>(MaxTrackedFiles);
    }

    /// <summary>
    /// Records a completed I/O operation and updates throughput estimates.
    /// </summary>
    /// <param name="filePath">The file path.</param>
    /// <param name="bytesRead">Number of bytes read.</param>
    /// <param name="elapsedMs">Time taken in milliseconds.</param>
    public void RecordThroughput(string filePath, long bytesRead, long elapsedMs)
    {
        if (elapsedMs <= 0 || bytesRead <= 0)
            return;

        var throughput = (bytesRead * 1000) / elapsedMs;

        // Update per-file tracker
        var tracker = _trackers.GetOrAdd(filePath, _ => new ThroughputTracker());
        tracker.RecordSample(throughput);

        // Update global estimate
        UpdateGlobalThroughput(throughput);
    }

    /// <summary>
    /// Gets the optimal prefetch size for a file based on observed throughput.
    /// </summary>
    /// <param name="filePath">The file path.</param>
    /// <param name="fileSize">The total file size.</param>
    /// <returns>The recommended prefetch size.</returns>
    public long GetOptimalPrefetchSize(string filePath, long fileSize)
    {
        long throughput;

        if (_trackers.TryGetValue(filePath, out var tracker) && tracker != null)
        {
            throughput = tracker.EstimatedThroughput;
        }
        else
        {
            lock (_globalLock)
            {
                throughput = _globalThroughputBytesPerSecond;
            }
        }

        // Calculate size that can be read within target latency
        var targetSize = (throughput * TargetLatencyMs) / 1000;

        // Clamp to reasonable bounds
        var prefetchSize = Math.Clamp(targetSize, MinPrefetchSize, MaxPrefetchSize);

        // Don't prefetch more than 10% of file
        var maxForFile = fileSize / 10;
        if (maxForFile > MinPrefetchSize)
        {
            prefetchSize = Math.Min(prefetchSize, maxForFile);
        }

        // Round to 64KB boundary for alignment
        return (prefetchSize / 65536) * 65536;
    }

    /// <summary>
    /// Gets the optimal buffer size for streaming based on throughput.
    /// </summary>
    public int GetOptimalBufferSize(string filePath)
    {
        long throughput;

        if (_trackers.TryGetValue(filePath, out var tracker) && tracker != null)
        {
            throughput = tracker.EstimatedThroughput;
        }
        else
        {
            lock (_globalLock)
            {
                throughput = _globalThroughputBytesPerSecond;
            }
        }

        // For high throughput (>100MB/s), use larger buffers
        if (throughput > 100 * 1024 * 1024)
            return 1024 * 1024; // 1MB

        // For medium throughput (>50MB/s), use 512KB
        if (throughput > 50 * 1024 * 1024)
            return 512 * 1024;

        // For lower throughput, use 256KB
        return 256 * 1024;
    }

    /// <summary>
    /// Gets the estimated throughput for a file or globally.
    /// </summary>
    public long GetEstimatedThroughput(string? filePath = null)
    {
        if (!string.IsNullOrEmpty(filePath) &&
            _trackers.TryGetValue(filePath, out var tracker) &&
            tracker != null)
        {
            return tracker.EstimatedThroughput;
        }

        lock (_globalLock)
        {
            return _globalThroughputBytesPerSecond;
        }
    }

    private void UpdateGlobalThroughput(long sample)
    {
        lock (_globalLock)
        {
            // Exponential moving average with bias toward higher values
            // (we're more interested in what the system can achieve)
            if (sample > _globalThroughputBytesPerSecond)
            {
                _globalThroughputBytesPerSecond = (_globalThroughputBytesPerSecond + sample * 3) / 4;
            }
            else
            {
                _globalThroughputBytesPerSecond = (_globalThroughputBytesPerSecond * 7 + sample) / 8;
            }
        }
    }

    /// <summary>
    /// Tracks throughput for a single file.
    /// </summary>
    private sealed class ThroughputTracker
    {
        private long _estimatedThroughput = 50 * 1024 * 1024; // 50 MB/s initial
        private int _sampleCount;
        private readonly object _lock = new();

        public long EstimatedThroughput
        {
            get
            {
                lock (_lock)
                {
                    return _estimatedThroughput;
                }
            }
        }

        public void RecordSample(long throughputBytesPerSecond)
        {
            lock (_lock)
            {
                _sampleCount++;

                if (_sampleCount == 1)
                {
                    _estimatedThroughput = throughputBytesPerSecond;
                }
                else
                {
                    // Exponential moving average with more weight on recent samples
                    _estimatedThroughput = (_estimatedThroughput * 3 + throughputBytesPerSecond) / 4;
                }
            }
        }
    }
}

/// <summary>
/// Utility for measuring I/O operation timing.
/// </summary>
internal static class IoTiming
{
    /// <summary>
    /// Executes an async operation and records throughput.
    /// </summary>
    public static async Task<T> MeasureAndRecordAsync<T>(
        string filePath,
        long bytesTransferred,
        Func<Task<T>> operation)
    {
        var sw = Stopwatch.StartNew();
        var result = await operation().ConfigureAwait(false);
        sw.Stop();

        if (bytesTransferred > 0)
        {
            AdaptivePrefetch.Instance.RecordThroughput(filePath, bytesTransferred, sw.ElapsedMilliseconds);
        }

        return result;
    }

    /// <summary>
    /// Executes an async operation and records throughput.
    /// </summary>
    public static async Task MeasureAndRecordAsync(
        string filePath,
        long bytesTransferred,
        Func<Task> operation)
    {
        var sw = Stopwatch.StartNew();
        await operation().ConfigureAwait(false);
        sw.Stop();

        if (bytesTransferred > 0)
        {
            AdaptivePrefetch.Instance.RecordThroughput(filePath, bytesTransferred, sw.ElapsedMilliseconds);
        }
    }
}
