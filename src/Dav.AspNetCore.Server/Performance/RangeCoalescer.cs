using System.Collections.Concurrent;

namespace Dav.AspNetCore.Server.Performance;

/// <summary>
/// Coalesces sequential range requests into larger reads for better I/O efficiency.
/// NZB clients often make many small sequential requests that can be batched.
/// </summary>
internal sealed class RangeCoalescer
{
    private static readonly Lazy<RangeCoalescer> LazyInstance = new(() => new RangeCoalescer());
    public static RangeCoalescer Instance => LazyInstance.Value;

    /// <summary>
    /// Maximum gap between ranges that can be coalesced (64KB).
    /// Small gaps are cheaper to over-read than to make separate requests.
    /// </summary>
    private const long MaxGap = 64 * 1024;

    /// <summary>
    /// Maximum size for a coalesced read (4MB).
    /// </summary>
    private const long MaxCoalescedSize = 4 * 1024 * 1024;

    /// <summary>
    /// Time window to collect ranges for coalescing.
    /// </summary>
    private static readonly TimeSpan CoalesceWindow = TimeSpan.FromMilliseconds(50);

    /// <summary>
    /// Maximum entries to track.
    /// </summary>
    private const int MaxTrackedFiles = 5000;

    private readonly LruCache<string, FileRangeTracker> _trackers;

    private RangeCoalescer()
    {
        _trackers = new LruCache<string, FileRangeTracker>(MaxTrackedFiles);
    }

    /// <summary>
    /// Records a range request and returns an optimized range that may include
    /// additional data for predicted subsequent requests.
    /// </summary>
    /// <param name="filePath">The file path.</param>
    /// <param name="requestedOffset">The requested offset.</param>
    /// <param name="requestedLength">The requested length.</param>
    /// <param name="fileSize">The total file size.</param>
    /// <returns>An optimized range (offset, length) that may be larger than requested.</returns>
    public (long Offset, long Length) GetOptimizedRange(
        string filePath,
        long requestedOffset,
        long requestedLength,
        long fileSize)
    {
        var tracker = _trackers.GetOrAdd(filePath, _ => new FileRangeTracker());
        return tracker.GetOptimizedRange(requestedOffset, requestedLength, fileSize);
    }

    /// <summary>
    /// Gets whether a file has sequential access patterns that benefit from read-ahead.
    /// </summary>
    public bool ShouldUseReadAhead(string filePath)
    {
        if (_trackers.TryGetValue(filePath, out var tracker) && tracker != null)
        {
            return tracker.IsSequentialPattern;
        }
        return true; // Default to read-ahead for new files
    }

    /// <summary>
    /// Invalidates tracking data for a file.
    /// </summary>
    public void Invalidate(string filePath)
    {
        _trackers.TryRemove(filePath, out _);
    }

    /// <summary>
    /// Tracks range requests for a single file.
    /// </summary>
    private sealed class FileRangeTracker
    {
        private long _lastOffset;
        private long _lastLength;
        private DateTime _lastAccess;
        private int _sequentialCount;
        private int _totalCount;
        private long _avgRequestSize;
        private readonly object _lock = new();

        /// <summary>
        /// The read-ahead multiplier based on observed patterns.
        /// Sequential access gets more aggressive read-ahead.
        /// </summary>
        public double ReadAheadMultiplier
        {
            get
            {
                lock (_lock)
                {
                    if (_totalCount < 3)
                        return 1.0;

                    var sequentialRatio = (double)_sequentialCount / _totalCount;
                    // Scale from 1x to 4x based on how sequential the access is
                    return 1.0 + (sequentialRatio * 3.0);
                }
            }
        }

        public bool IsSequentialPattern
        {
            get
            {
                lock (_lock)
                {
                    return _totalCount < 3 || _sequentialCount > _totalCount / 2;
                }
            }
        }

        public (long Offset, long Length) GetOptimizedRange(
            long requestedOffset,
            long requestedLength,
            long fileSize)
        {
            lock (_lock)
            {
                var now = DateTime.UtcNow;

                // Expire old data
                if (now - _lastAccess > TimeSpan.FromMinutes(5))
                {
                    _sequentialCount = 0;
                    _totalCount = 0;
                    _avgRequestSize = requestedLength;
                }

                _totalCount++;

                // Check if this is sequential access
                var expectedNextOffset = _lastOffset + _lastLength;
                var gap = requestedOffset - expectedNextOffset;

                if (Math.Abs(gap) <= MaxGap)
                {
                    _sequentialCount++;
                }

                // Update average request size (exponential moving average)
                _avgRequestSize = (_avgRequestSize * 3 + requestedLength) / 4;

                _lastOffset = requestedOffset;
                _lastLength = requestedLength;
                _lastAccess = now;

                // For sequential patterns, extend the read with read-ahead
                if (IsSequentialPattern && _totalCount >= 2)
                {
                    var readAheadSize = (long)(_avgRequestSize * ReadAheadMultiplier);
                    var optimizedLength = Math.Min(
                        requestedLength + readAheadSize,
                        MaxCoalescedSize);

                    // Don't exceed file size
                    optimizedLength = Math.Min(optimizedLength, fileSize - requestedOffset);

                    return (requestedOffset, optimizedLength);
                }

                return (requestedOffset, requestedLength);
            }
        }
    }
}

/// <summary>
/// Represents a coalesced range with the original request embedded.
/// </summary>
public readonly struct CoalescedRange
{
    /// <summary>
    /// The optimized range start offset.
    /// </summary>
    public long Offset { get; }

    /// <summary>
    /// The optimized range length (may be larger than requested).
    /// </summary>
    public long Length { get; }

    /// <summary>
    /// The originally requested offset.
    /// </summary>
    public long RequestedOffset { get; }

    /// <summary>
    /// The originally requested length.
    /// </summary>
    public long RequestedLength { get; }

    public CoalescedRange(long offset, long length, long requestedOffset, long requestedLength)
    {
        Offset = offset;
        Length = length;
        RequestedOffset = requestedOffset;
        RequestedLength = requestedLength;
    }

    /// <summary>
    /// Whether the range was extended beyond the original request.
    /// </summary>
    public bool WasCoalesced => Length > RequestedLength;
}
