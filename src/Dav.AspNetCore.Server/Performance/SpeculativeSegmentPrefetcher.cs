using System.Collections.Concurrent;
using System.Threading.Channels;

namespace Dav.AspNetCore.Server.Performance;

/// <summary>
/// Performs speculative prefetching based on detected streaming patterns.
/// Optimized for NZB/video streaming where access patterns are predictable:
/// - Initial header read (first ~1MB)
/// - Sequential streaming from near the start
/// - Seeks to specific positions followed by sequential reads
/// </summary>
internal sealed class SpeculativeSegmentPrefetcher : IDisposable
{
    private static readonly Lazy<SpeculativeSegmentPrefetcher> LazyInstance = new(() => new SpeculativeSegmentPrefetcher());
    public static SpeculativeSegmentPrefetcher Instance => LazyInstance.Value;

    /// <summary>
    /// Segment size for prefetching (2MB).
    /// </summary>
    public const long SegmentSize = 2 * 1024 * 1024;

    /// <summary>
    /// Number of segments to prefetch ahead.
    /// </summary>
    public const int SegmentsAhead = 3;

    /// <summary>
    /// Maximum concurrent prefetch operations.
    /// </summary>
    private const int MaxConcurrentPrefetches = 6;

    /// <summary>
    /// Maximum files to track.
    /// </summary>
    private const int MaxTrackedFiles = 1000;

    private readonly Channel<SegmentRequest> _requestChannel;
    private readonly LruCache<string, FileStreamState> _fileStates;
    private readonly ConcurrentDictionary<string, DateTime> _recentPrefetches;
    private readonly SemaphoreSlim _concurrencySemaphore;
    private readonly Task[] _workerTasks;
    private readonly CancellationTokenSource _cts;
    private bool _disposed;

    private static readonly TimeSpan MinPrefetchInterval = TimeSpan.FromMilliseconds(100);

    private SpeculativeSegmentPrefetcher()
    {
        _requestChannel = Channel.CreateBounded<SegmentRequest>(new BoundedChannelOptions(200)
        {
            FullMode = BoundedChannelFullMode.DropOldest,
            SingleReader = false,
            SingleWriter = false
        });

        _fileStates = new LruCache<string, FileStreamState>(MaxTrackedFiles);
        _recentPrefetches = new ConcurrentDictionary<string, DateTime>();
        _concurrencySemaphore = new SemaphoreSlim(MaxConcurrentPrefetches, MaxConcurrentPrefetches);
        _cts = new CancellationTokenSource();

        // Start worker tasks
        _workerTasks = new Task[MaxConcurrentPrefetches];
        for (int i = 0; i < MaxConcurrentPrefetches; i++)
        {
            _workerTasks[i] = Task.Run(() => ProcessRequestsAsync(_cts.Token));
        }
    }

    /// <summary>
    /// Called when a read operation completes. Updates streaming state and triggers prefetch.
    /// </summary>
    /// <param name="filePath">The physical file path.</param>
    /// <param name="offset">The read offset.</param>
    /// <param name="length">The read length.</param>
    /// <param name="fileSize">The total file size.</param>
    /// <param name="elapsedMs">How long the read took.</param>
    public void OnReadComplete(string filePath, long offset, long length, long fileSize, long elapsedMs)
    {
        if (_disposed || string.IsNullOrEmpty(filePath))
            return;

        // Update file streaming state
        var state = _fileStates.GetOrAdd(filePath, _ => new FileStreamState(fileSize));
        var pattern = state.RecordAccess(offset, length, elapsedMs);

        // Trigger speculative prefetch based on pattern
        TriggerSpeculativePrefetch(filePath, state, pattern);
    }

    /// <summary>
    /// Notifies the prefetcher that a stream is starting.
    /// </summary>
    public void OnStreamStart(string filePath, long fileSize)
    {
        if (_disposed || string.IsNullOrEmpty(filePath))
            return;

        var state = _fileStates.GetOrAdd(filePath, _ => new FileStreamState(fileSize));

        // For new streams, prefetch the first few segments
        QueuePrefetch(filePath, 0, SegmentSize * SegmentsAhead, fileSize, SegmentPriority.High);

        // Also trigger parallel chunk prefetch for initial data
        ParallelChunkPrefetcher.Instance.TriggerInitialPrefetch(filePath, fileSize);
    }

    /// <summary>
    /// Notifies the prefetcher of a seek operation.
    /// </summary>
    public void OnSeek(string filePath, long newOffset, long fileSize)
    {
        if (_disposed || string.IsNullOrEmpty(filePath))
            return;

        var state = _fileStates.GetOrAdd(filePath, _ => new FileStreamState(fileSize));
        state.RecordSeek(newOffset);

        // After a seek, prefetch segments ahead of the new position
        QueuePrefetch(filePath, newOffset, SegmentSize * SegmentsAhead, fileSize, SegmentPriority.High);

        // Trigger parallel chunk prefetch for the seek position
        ParallelChunkPrefetcher.Instance.TriggerRangePrefetch(filePath, fileSize, newOffset, 3);
    }

    private void TriggerSpeculativePrefetch(string filePath, FileStreamState state, StreamingPattern pattern)
    {
        switch (pattern)
        {
            case StreamingPattern.Sequential:
                // For sequential, prefetch several segments ahead
                var nextOffset = state.LastEndOffset;
                QueuePrefetch(filePath, nextOffset, SegmentSize * SegmentsAhead, state.FileSize, SegmentPriority.Normal);
                break;

            case StreamingPattern.VideoScrubbing:
                // For scrubbing, prefetch in the predicted direction
                var predictedOffset = state.PredictNextOffset();
                if (predictedOffset >= 0)
                {
                    QueuePrefetch(filePath, predictedOffset, SegmentSize * 2, state.FileSize, SegmentPriority.Normal);
                }
                break;

            case StreamingPattern.RandomAccess:
                // For random access, less aggressive prefetching
                var currentEnd = state.LastEndOffset;
                QueuePrefetch(filePath, currentEnd, SegmentSize, state.FileSize, SegmentPriority.Low);
                break;
        }
    }

    private void QueuePrefetch(string filePath, long offset, long length, long fileSize, SegmentPriority priority)
    {
        if (_disposed)
            return;

        // Clamp to file bounds
        if (offset >= fileSize)
            return;

        length = Math.Min(length, fileSize - offset);
        if (length <= 0)
            return;

        // Check if we recently prefetched this region
        var cacheKey = $"{filePath}:{offset / SegmentSize}";
        var now = DateTime.UtcNow;

        if (_recentPrefetches.TryGetValue(cacheKey, out var lastPrefetch))
        {
            if (now - lastPrefetch < MinPrefetchInterval)
                return;
        }

        _recentPrefetches[cacheKey] = now;

        var request = new SegmentRequest(filePath, offset, length, priority);
        _requestChannel.Writer.TryWrite(request);
    }

    private async Task ProcessRequestsAsync(CancellationToken cancellationToken)
    {
        await foreach (var request in _requestChannel.Reader.ReadAllAsync(cancellationToken))
        {
            try
            {
                await _concurrencySemaphore.WaitAsync(cancellationToken);
                try
                {
                    await PrefetchSegmentAsync(request, cancellationToken);
                }
                finally
                {
                    _concurrencySemaphore.Release();
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch
            {
                // Non-critical - continue with next request
            }
        }
    }

    private static async Task PrefetchSegmentAsync(SegmentRequest request, CancellationToken cancellationToken)
    {
        if (!File.Exists(request.FilePath))
            return;

        try
        {
            // Use Linux kernel hints for prefetching
            if (LinuxKernelHints.IsAvailable)
            {
                LinuxKernelHints.PrefetchFileRange(request.FilePath, request.Offset, request.Length);
            }

            // Read data to bring it into OS cache
            await using var stream = new FileStream(
                request.FilePath,
                FileMode.Open,
                FileAccess.Read,
                FileShare.Read,
                bufferSize: 256 * 1024,
                FileOptions.Asynchronous | FileOptions.SequentialScan);

            if (request.Offset >= stream.Length)
                return;

            stream.Seek(request.Offset, SeekOrigin.Begin);

            // Apply kernel hints
            if (LinuxKernelHints.IsAvailable)
            {
                var fd = LinuxKernelHints.GetFileDescriptor(stream);
                if (fd >= 0)
                {
                    LinuxKernelHints.AdviseSequential(fd, request.Offset, request.Length);
                    LinuxKernelHints.AdviseWillNeed(fd, request.Offset, request.Length);
                }
            }

            var buffer = BufferPool.Rent(256 * 1024);
            try
            {
                var remaining = Math.Min(request.Length, stream.Length - request.Offset);
                while (remaining > 0 && !cancellationToken.IsCancellationRequested)
                {
                    var toRead = (int)Math.Min(remaining, buffer.Length);
                    var bytesRead = await stream.ReadAsync(buffer.AsMemory(0, toRead), cancellationToken);

                    if (bytesRead == 0)
                        break;

                    remaining -= bytesRead;
                }
            }
            finally
            {
                BufferPool.Return(buffer);
            }
        }
        catch (IOException)
        {
            // File may be locked or unavailable - not critical
        }
    }

    /// <summary>
    /// Invalidates tracking data for a file.
    /// </summary>
    public void Invalidate(string filePath)
    {
        _fileStates.TryRemove(filePath, out _);
    }

    /// <summary>
    /// Cleans up old tracking data.
    /// </summary>
    public void Cleanup()
    {
        var cutoff = DateTime.UtcNow - TimeSpan.FromMinutes(10);
        var keysToRemove = _recentPrefetches
            .Where(kvp => kvp.Value < cutoff)
            .Select(kvp => kvp.Key)
            .ToList();

        foreach (var key in keysToRemove)
        {
            _recentPrefetches.TryRemove(key, out _);
        }
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        _disposed = true;
        _cts.Cancel();
        _requestChannel.Writer.Complete();

        try
        {
            Task.WaitAll(_workerTasks, TimeSpan.FromSeconds(5));
        }
        catch
        {
            // Ignore shutdown errors
        }

        _cts.Dispose();
        _concurrencySemaphore.Dispose();
    }

    private readonly struct SegmentRequest
    {
        public string FilePath { get; }
        public long Offset { get; }
        public long Length { get; }
        public SegmentPriority Priority { get; }

        public SegmentRequest(string filePath, long offset, long length, SegmentPriority priority)
        {
            FilePath = filePath;
            Offset = offset;
            Length = length;
            Priority = priority;
        }
    }

    private enum SegmentPriority
    {
        High,
        Normal,
        Low
    }

    /// <summary>
    /// Detected streaming patterns.
    /// </summary>
    private enum StreamingPattern
    {
        Unknown,
        Sequential,
        VideoScrubbing,
        RandomAccess
    }

    /// <summary>
    /// Tracks streaming state for a single file.
    /// </summary>
    private sealed class FileStreamState
    {
        public long FileSize { get; }
        public long LastOffset { get; private set; }
        public long LastLength { get; private set; }
        public long LastEndOffset => LastOffset + LastLength;

        private long _totalBytesRead;
        private int _accessCount;
        private int _sequentialCount;
        private int _forwardSeekCount;
        private int _backwardSeekCount;
        private long _avgSeekDistance;
        private long _avgThroughput;
        private DateTime _lastAccess;
        private readonly object _lock = new();

        public FileStreamState(long fileSize)
        {
            FileSize = fileSize;
            _lastAccess = DateTime.UtcNow;
        }

        public StreamingPattern RecordAccess(long offset, long length, long elapsedMs)
        {
            lock (_lock)
            {
                var now = DateTime.UtcNow;

                // Reset if too old
                if (now - _lastAccess > TimeSpan.FromMinutes(5))
                {
                    _accessCount = 0;
                    _sequentialCount = 0;
                    _forwardSeekCount = 0;
                    _backwardSeekCount = 0;
                }

                _accessCount++;
                _totalBytesRead += length;

                // Update throughput estimate
                if (elapsedMs > 0)
                {
                    var throughput = (length * 1000) / elapsedMs;
                    _avgThroughput = _accessCount == 1
                        ? throughput
                        : (_avgThroughput * 3 + throughput) / 4;
                }

                // Analyze access pattern
                var expectedNextOffset = LastOffset + LastLength;
                var seekDistance = offset - expectedNextOffset;

                if (_accessCount > 1)
                {
                    if (Math.Abs(seekDistance) < 64 * 1024) // Within 64KB
                    {
                        _sequentialCount++;
                    }
                    else if (seekDistance > 0 && seekDistance < 10 * 1024 * 1024) // Forward < 10MB
                    {
                        _forwardSeekCount++;
                        _avgSeekDistance = (_avgSeekDistance * 3 + seekDistance) / 4;
                    }
                    else if (seekDistance < 0 && Math.Abs(seekDistance) < 10 * 1024 * 1024) // Backward < 10MB
                    {
                        _backwardSeekCount++;
                    }
                }

                LastOffset = offset;
                LastLength = length;
                _lastAccess = now;

                return DetectPattern();
            }
        }

        public void RecordSeek(long newOffset)
        {
            lock (_lock)
            {
                var seekDistance = newOffset - LastEndOffset;
                if (seekDistance > 0)
                    _forwardSeekCount++;
                else if (seekDistance < 0)
                    _backwardSeekCount++;

                LastOffset = newOffset;
                _lastAccess = DateTime.UtcNow;
            }
        }

        private StreamingPattern DetectPattern()
        {
            if (_accessCount < 3)
                return StreamingPattern.Unknown;

            var total = _sequentialCount + _forwardSeekCount + _backwardSeekCount;
            if (total == 0)
                return StreamingPattern.Sequential;

            var sequentialRatio = (double)_sequentialCount / total;
            var forwardRatio = (double)_forwardSeekCount / total;

            if (sequentialRatio > 0.7)
                return StreamingPattern.Sequential;

            if (forwardRatio > 0.5 || (_forwardSeekCount > _backwardSeekCount * 2))
                return StreamingPattern.VideoScrubbing;

            return StreamingPattern.RandomAccess;
        }

        public long PredictNextOffset()
        {
            lock (_lock)
            {
                if (_avgSeekDistance > 0 && _forwardSeekCount > _backwardSeekCount)
                {
                    return LastEndOffset + _avgSeekDistance;
                }
                return LastEndOffset;
            }
        }
    }
}
