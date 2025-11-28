using System.Collections.Concurrent;
using System.Runtime.InteropServices;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Features;

namespace Dav.AspNetCore.Server.Performance;

/// <summary>
/// Coordinates all NZB streaming optimizations for maximum performance.
/// Provides aggressive early-response patterns, predictive prefetching,
/// and instant-response capabilities for streaming workloads.
///
/// Key features:
/// - Instant response mode for hot files (sub-millisecond TTFB)
/// - Aggressive header pre-flushing
/// - Predictive seek caching
/// - Connection warmup coordination
/// - Multi-layer caching orchestration
/// </summary>
internal sealed class NzbStreamingOptimizer
{
    private static readonly Lazy<NzbStreamingOptimizer> LazyInstance = new(() => new NzbStreamingOptimizer());
    public static NzbStreamingOptimizer Instance => LazyInstance.Value;

    /// <summary>
    /// Threshold for considering a file as "hot" (accessed multiple times recently).
    /// </summary>
    private const int HotFileThreshold = 2;

    /// <summary>
    /// Maximum number of hot files to track.
    /// </summary>
    private const int MaxHotFiles = 200;

    /// <summary>
    /// Window for tracking hot file accesses.
    /// </summary>
    private static readonly TimeSpan HotFileWindow = TimeSpan.FromMinutes(5);

    private readonly LruCache<string, HotFileState> _hotFiles;
    private readonly ConcurrentDictionary<string, ClientStreamState> _clientStreams;
    private readonly Timer _cleanupTimer;

    private NzbStreamingOptimizer()
    {
        _hotFiles = new LruCache<string, HotFileState>(MaxHotFiles);
        _clientStreams = new ConcurrentDictionary<string, ClientStreamState>();
        _cleanupTimer = new Timer(CleanupCallback, null, TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(1));
    }

    /// <summary>
    /// Prepares the context for optimal NZB streaming.
    /// Call this at the start of request processing.
    /// </summary>
    public StreamingPreparation PrepareForStreaming(
        HttpContext context,
        string physicalPath,
        long contentLength,
        bool isRangeRequest)
    {
        var prep = new StreamingPreparation();

        // Check if this is a hot file with instant response capability
        var hotFileState = UpdateHotFileState(physicalPath);
        prep.IsHotFile = hotFileState.IsHot;

        // Try to get instant response cache entry
        prep.InstantEntry = InstantResponseCache.Instance.TryGetInstant(physicalPath);

        // Determine optimal streaming strategy
        prep.Strategy = DetermineStrategy(physicalPath, contentLength, isRangeRequest, hotFileState);

        // Track client streaming state for prediction
        var clientId = GetClientId(context);
        prep.ClientState = GetOrCreateClientState(clientId, physicalPath);

        // Apply socket tuning based on strategy
        ApplySocketOptimizations(context, contentLength, prep.Strategy);

        // Pre-warm next predicted seeks
        if (prep.ClientState != null && prep.Strategy == StreamingStrategy.AggressiveStream)
        {
            PreWarmPredictedSeeks(physicalPath, prep.ClientState, contentLength);
        }

        return prep;
    }

    /// <summary>
    /// Applies instant response headers when available.
    /// Returns true if instant headers were applied.
    /// </summary>
    public bool TryApplyInstantHeaders(
        HttpContext context,
        StreamingPreparation prep,
        long? rangeOffset = null,
        long? rangeLength = null)
    {
        if (prep.InstantEntry == null)
            return false;

        // Apply pre-computed headers
        if (rangeOffset.HasValue && rangeLength.HasValue)
        {
            prep.InstantEntry.ApplyRangeHeaders(context.Response, rangeOffset.Value, rangeLength.Value);
        }
        else
        {
            prep.InstantEntry.ApplyHeaders(context.Response);
        }

        return true;
    }

    /// <summary>
    /// Flushes response headers immediately for fastest TTFB.
    /// Call after setting all headers but before body write.
    /// </summary>
    public async Task FlushHeadersInstantAsync(HttpContext context, CancellationToken cancellationToken)
    {
        // Disable output buffering
        var bufferingFeature = context.Features.Get<IHttpResponseBodyFeature>();
        bufferingFeature?.DisableBuffering();

        // Start the response to flush headers immediately
        await context.Response.StartAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Records a successful stream completion for learning.
    /// </summary>
    public void RecordStreamComplete(
        string physicalPath,
        long contentLength,
        string? contentType,
        string? etag,
        DateTimeOffset lastModified,
        long bytesTransferred,
        long elapsedMs)
    {
        // Update instant response cache for future requests
        InstantResponseCache.Instance.RecordAccess(
            physicalPath, contentLength, contentType, etag, lastModified);

        // Update hot file state
        var state = _hotFiles.GetOrAdd(physicalPath, _ => new HotFileState());
        state.RecordSuccess(bytesTransferred, elapsedMs);
    }

    /// <summary>
    /// Records a seek operation for pattern learning.
    /// </summary>
    public void RecordSeek(HttpContext context, string physicalPath, long offset, long length)
    {
        var clientId = GetClientId(context);
        var clientState = GetOrCreateClientState(clientId, physicalPath);
        clientState?.RecordSeek(offset, length);

        // Update hot file state
        if (_hotFiles.TryGetValue(physicalPath, out var hotState) && hotState != null)
        {
            hotState.RecordSeek(offset);
        }
    }

    /// <summary>
    /// Predicts the next seek position based on client patterns.
    /// </summary>
    public SeekPrediction? PredictNextSeek(HttpContext context, string physicalPath, long currentOffset, long contentLength)
    {
        var clientId = GetClientId(context);
        if (!_clientStreams.TryGetValue($"{clientId}:{physicalPath}", out var clientState) || clientState == null)
            return null;

        return clientState.PredictNextSeek(currentOffset, contentLength);
    }

    /// <summary>
    /// Gets the optimal file handle for streaming.
    /// Uses cached handles for hot files.
    /// </summary>
    public CachedFileHandle? GetOptimalHandle(string physicalPath, FileAccessPattern pattern)
    {
        // For hot files, use cached handles
        if (_hotFiles.TryGetValue(physicalPath, out var state) && state != null && state.IsHot)
        {
            return FileHandleCache.Instance.GetHandle(physicalPath, pattern);
        }

        // For other files, open normally but track for caching
        return FileHandleCache.Instance.GetHandle(physicalPath, pattern);
    }

    /// <summary>
    /// Pre-warms caches for a file that is about to be accessed.
    /// </summary>
    public void PreWarmFile(string physicalPath, long contentLength)
    {
        // Pre-warm instant response cache
        InstantResponseCache.Instance.PreWarm(physicalPath);

        // Pre-warm file handle cache for hot files
        if (_hotFiles.TryGetValue(physicalPath, out var state) && state != null && state.IsHot)
        {
            FileHandleCache.Instance.PreOpen(physicalPath, FileAccessPattern.Sequential);
        }

        // Trigger kernel prefetch for initial segments
        if (LinuxKernelHints.IsAvailable)
        {
            LinuxKernelHints.PrefetchFileRange(physicalPath, 0, Math.Min(contentLength, 8 * 1024 * 1024));
        }
    }

    private HotFileState UpdateHotFileState(string physicalPath)
    {
        var state = _hotFiles.GetOrAdd(physicalPath, _ => new HotFileState());
        state.RecordAccess();
        return state;
    }

    private StreamingStrategy DetermineStrategy(
        string physicalPath,
        long contentLength,
        bool isRangeRequest,
        HotFileState hotState)
    {
        // Hot files get aggressive optimization
        if (hotState.IsHot)
        {
            return isRangeRequest
                ? StreamingStrategy.InstantSeek
                : StreamingStrategy.AggressiveStream;
        }

        // Large files get streaming optimization
        if (contentLength > 50 * 1024 * 1024)
        {
            return StreamingStrategy.AggressiveStream;
        }

        // Range requests get seek optimization
        if (isRangeRequest)
        {
            return StreamingStrategy.OptimizedSeek;
        }

        // Default streaming
        return StreamingStrategy.Standard;
    }

    private void ApplySocketOptimizations(HttpContext context, long contentLength, StreamingStrategy strategy)
    {
        switch (strategy)
        {
            case StreamingStrategy.InstantSeek:
                // Ultra-low latency for instant seeks
                StreamingConnectionTuner.PrepareForFastFirstByte(context);
                break;

            case StreamingStrategy.AggressiveStream:
                // High throughput for streaming
                StreamingConnectionTuner.WarmupConnection(context, contentLength);
                break;

            case StreamingStrategy.OptimizedSeek:
                // Balance latency and throughput
                StreamingConnectionTuner.TuneForStreaming(context, contentLength, true);
                break;

            default:
                // Standard tuning
                StreamingConnectionTuner.TuneForStreaming(context, contentLength, false);
                break;
        }
    }

    private void PreWarmPredictedSeeks(string physicalPath, ClientStreamState clientState, long contentLength)
    {
        var predictions = clientState.GetPredictedPositions(contentLength, 3);

        foreach (var position in predictions)
        {
            // Queue prefetch for predicted positions
            PrefetchService.Instance.QueuePrefetch(physicalPath, position, 2 * 1024 * 1024);

            // Use kernel hints
            if (LinuxKernelHints.IsAvailable)
            {
                LinuxKernelHints.PrefetchFileRange(physicalPath, position, 2 * 1024 * 1024);
            }
        }
    }

    private static string GetClientId(HttpContext context)
    {
        // Use connection ID + remote IP as client identifier
        return $"{context.Connection.RemoteIpAddress}:{context.Connection.RemotePort}";
    }

    private ClientStreamState? GetOrCreateClientState(string clientId, string physicalPath)
    {
        var key = $"{clientId}:{physicalPath}";
        return _clientStreams.GetOrAdd(key, _ => new ClientStreamState());
    }

    private void CleanupCallback(object? state)
    {
        try
        {
            var now = DateTime.UtcNow;

            // Clean up old client states
            var keysToRemove = _clientStreams
                .Where(kvp => kvp.Value.IsExpired)
                .Select(kvp => kvp.Key)
                .ToList();

            foreach (var key in keysToRemove)
            {
                _clientStreams.TryRemove(key, out _);
            }
        }
        catch
        {
            // Non-critical cleanup
        }
    }

    /// <summary>
    /// Streaming strategy based on file and access patterns.
    /// </summary>
    public enum StreamingStrategy
    {
        /// <summary>Standard streaming with basic optimizations.</summary>
        Standard,

        /// <summary>Aggressive streaming for large files with prefetching.</summary>
        AggressiveStream,

        /// <summary>Optimized seeking for range requests.</summary>
        OptimizedSeek,

        /// <summary>Instant seek for hot files with cached metadata.</summary>
        InstantSeek
    }

    /// <summary>
    /// State for a hot file that's accessed frequently.
    /// </summary>
    private sealed class HotFileState
    {
        private int _accessCount;
        private DateTime _firstAccess;
        private DateTime _lastAccess;
        private long _totalBytesTransferred;
        private int _seekCount;
        private long _avgSeekDistance;
        private readonly object _lock = new();

        public bool IsHot
        {
            get
            {
                lock (_lock)
                {
                    if (_accessCount < HotFileThreshold)
                        return false;

                    // Must have been accessed recently
                    return DateTime.UtcNow - _lastAccess < HotFileWindow;
                }
            }
        }

        public void RecordAccess()
        {
            lock (_lock)
            {
                if (_accessCount == 0)
                    _firstAccess = DateTime.UtcNow;

                _accessCount++;
                _lastAccess = DateTime.UtcNow;
            }
        }

        public void RecordSuccess(long bytesTransferred, long elapsedMs)
        {
            lock (_lock)
            {
                _totalBytesTransferred += bytesTransferred;
                _lastAccess = DateTime.UtcNow;
            }
        }

        public void RecordSeek(long offset)
        {
            lock (_lock)
            {
                _seekCount++;
            }
        }
    }

    /// <summary>
    /// Tracks streaming state for a single client connection.
    /// </summary>
    private sealed class ClientStreamState
    {
        private readonly List<SeekRecord> _seekHistory = new();
        private readonly object _lock = new();
        private DateTime _lastActivity = DateTime.UtcNow;
        private long _lastOffset;
        private long _avgReadSize;
        private int _readCount;

        private static readonly TimeSpan ExpirationTime = TimeSpan.FromMinutes(5);

        public bool IsExpired => DateTime.UtcNow - _lastActivity > ExpirationTime;

        public void RecordSeek(long offset, long length)
        {
            lock (_lock)
            {
                _lastActivity = DateTime.UtcNow;
                _seekHistory.Add(new SeekRecord(offset, length, _lastActivity));

                // Keep only recent history
                while (_seekHistory.Count > 50)
                    _seekHistory.RemoveAt(0);

                // Update averages
                _readCount++;
                _avgReadSize = (_avgReadSize * (_readCount - 1) + length) / _readCount;
                _lastOffset = offset + length;
            }
        }

        public SeekPrediction? PredictNextSeek(long currentOffset, long contentLength)
        {
            lock (_lock)
            {
                if (_seekHistory.Count < 2)
                    return null;

                // Analyze recent seeks to predict pattern
                var recentSeeks = _seekHistory.TakeLast(5).ToList();

                // Check for sequential pattern
                var isSequential = true;
                for (int i = 1; i < recentSeeks.Count; i++)
                {
                    var expectedOffset = recentSeeks[i - 1].Offset + recentSeeks[i - 1].Length;
                    if (Math.Abs(recentSeeks[i].Offset - expectedOffset) > 64 * 1024)
                    {
                        isSequential = false;
                        break;
                    }
                }

                if (isSequential)
                {
                    // Predict next sequential position
                    var predictedOffset = currentOffset + _avgReadSize;
                    if (predictedOffset < contentLength)
                    {
                        return new SeekPrediction(predictedOffset, _avgReadSize, SeekPattern.Sequential, 0.9);
                    }
                }

                // Check for forward scrubbing pattern
                var avgStride = CalculateAverageStride(recentSeeks);
                if (avgStride > 0 && avgStride < contentLength / 10)
                {
                    var predictedOffset = currentOffset + avgStride;
                    if (predictedOffset < contentLength)
                    {
                        return new SeekPrediction(predictedOffset, _avgReadSize, SeekPattern.ForwardScrub, 0.7);
                    }
                }

                return null;
            }
        }

        public IReadOnlyList<long> GetPredictedPositions(long contentLength, int count)
        {
            lock (_lock)
            {
                var predictions = new List<long>();

                if (_seekHistory.Count < 2)
                {
                    // Default to segments from current position
                    var pos = _lastOffset;
                    for (int i = 0; i < count && pos < contentLength; i++)
                    {
                        predictions.Add(pos);
                        pos += 2 * 1024 * 1024; // 2MB segments
                    }
                    return predictions;
                }

                // Use prediction logic
                var prediction = PredictNextSeek(_lastOffset, contentLength);
                if (prediction.HasValue)
                {
                    var pos = prediction.Value.Offset;
                    for (int i = 0; i < count && pos < contentLength; i++)
                    {
                        predictions.Add(pos);
                        pos += (long)_avgReadSize;
                    }
                }

                return predictions;
            }
        }

        private static long CalculateAverageStride(List<SeekRecord> seeks)
        {
            if (seeks.Count < 2)
                return 0;

            long totalStride = 0;
            for (int i = 1; i < seeks.Count; i++)
            {
                totalStride += seeks[i].Offset - seeks[i - 1].Offset;
            }
            return totalStride / (seeks.Count - 1);
        }

        private readonly record struct SeekRecord(long Offset, long Length, DateTime Time);
    }
}

/// <summary>
/// Preparation data for streaming a file.
/// </summary>
public struct StreamingPreparation
{
    /// <summary>Whether this is a frequently accessed hot file.</summary>
    public bool IsHotFile { get; set; }

    /// <summary>Instant response cache entry if available.</summary>
    public InstantResponseEntry? InstantEntry { get; set; }

    /// <summary>The streaming strategy to use.</summary>
    public NzbStreamingOptimizer.StreamingStrategy Strategy { get; set; }

    /// <summary>Client streaming state for predictions.</summary>
    internal object? ClientState { get; set; }
}

/// <summary>
/// A predicted seek position.
/// </summary>
public readonly struct SeekPrediction
{
    /// <summary>Predicted offset position.</summary>
    public long Offset { get; }

    /// <summary>Predicted read size.</summary>
    public long Size { get; }

    /// <summary>Detected seek pattern.</summary>
    public SeekPattern Pattern { get; }

    /// <summary>Confidence in prediction (0-1).</summary>
    public double Confidence { get; }

    public SeekPrediction(long offset, long size, SeekPattern pattern, double confidence)
    {
        Offset = offset;
        Size = size;
        Pattern = pattern;
        Confidence = confidence;
    }
}

/// <summary>
/// Detected seek pattern types.
/// </summary>
public enum SeekPattern
{
    /// <summary>Unknown or unpredictable pattern.</summary>
    Unknown,

    /// <summary>Sequential forward reading.</summary>
    Sequential,

    /// <summary>Forward scrubbing (like video timeline).</summary>
    ForwardScrub,

    /// <summary>Backward scrubbing.</summary>
    BackwardScrub,

    /// <summary>Random access pattern.</summary>
    Random
}
