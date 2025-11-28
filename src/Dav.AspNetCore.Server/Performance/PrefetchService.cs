using System.Collections.Concurrent;
using System.Threading.Channels;

namespace Dav.AspNetCore.Server.Performance;

/// <summary>
/// Background service that prefetches file data into OS cache based on access pattern predictions.
/// This significantly speeds up subsequent seek operations by ensuring data is ready.
/// </summary>
internal sealed class PrefetchService : IDisposable
{
    private static readonly Lazy<PrefetchService> LazyInstance = new(() => new PrefetchService());
    public static PrefetchService Instance => LazyInstance.Value;

    /// <summary>
    /// Maximum concurrent prefetch operations.
    /// </summary>
    private const int MaxConcurrentPrefetches = 4;

    /// <summary>
    /// Maximum queued prefetch requests.
    /// </summary>
    private const int MaxQueuedRequests = 100;

    /// <summary>
    /// Prefetch buffer size (256KB - enough to trigger OS read-ahead).
    /// </summary>
    private const int PrefetchBufferSize = 256 * 1024;

    private readonly Channel<PrefetchRequest> _prefetchChannel;
    private readonly ConcurrentDictionary<string, DateTime> _recentPrefetches;
    private readonly Task[] _workerTasks;
    private readonly CancellationTokenSource _cts;
    private bool _disposed;

    /// <summary>
    /// Minimum time between prefetches for the same file region.
    /// </summary>
    private static readonly TimeSpan MinPrefetchInterval = TimeSpan.FromSeconds(5);

    private PrefetchService()
    {
        _prefetchChannel = Channel.CreateBounded<PrefetchRequest>(new BoundedChannelOptions(MaxQueuedRequests)
        {
            FullMode = BoundedChannelFullMode.DropOldest,
            SingleReader = false,
            SingleWriter = false
        });

        _recentPrefetches = new ConcurrentDictionary<string, DateTime>();
        _cts = new CancellationTokenSource();

        // Start worker tasks
        _workerTasks = new Task[MaxConcurrentPrefetches];
        for (int i = 0; i < MaxConcurrentPrefetches; i++)
        {
            _workerTasks[i] = Task.Run(() => ProcessPrefetchesAsync(_cts.Token));
        }
    }

    /// <summary>
    /// Queues a prefetch request based on pattern prediction.
    /// The prefetch will read the data into OS cache, making future seeks faster.
    /// </summary>
    /// <param name="filePath">The physical file path.</param>
    /// <param name="hint">The prefetch hint from pattern tracking.</param>
    /// <returns>True if the request was queued, false if skipped.</returns>
    public bool QueuePrefetch(string filePath, PrefetchHint hint)
    {
        if (_disposed || string.IsNullOrEmpty(filePath))
            return false;

        // Skip if we recently prefetched this region
        var cacheKey = $"{filePath}:{hint.PredictedOffset / (PrefetchBufferSize * 4)}";
        var now = DateTime.UtcNow;

        if (_recentPrefetches.TryGetValue(cacheKey, out var lastPrefetch))
        {
            if (now - lastPrefetch < MinPrefetchInterval)
                return false;
        }

        _recentPrefetches[cacheKey] = now;

        // Try to queue the prefetch
        var request = new PrefetchRequest(filePath, hint.PredictedOffset, hint.PrefetchSize);
        return _prefetchChannel.Writer.TryWrite(request);
    }

    /// <summary>
    /// Queues a prefetch for a specific file region.
    /// </summary>
    public bool QueuePrefetch(string filePath, long offset, long length)
    {
        if (_disposed || string.IsNullOrEmpty(filePath))
            return false;

        var cacheKey = $"{filePath}:{offset / (PrefetchBufferSize * 4)}";
        var now = DateTime.UtcNow;

        if (_recentPrefetches.TryGetValue(cacheKey, out var lastPrefetch))
        {
            if (now - lastPrefetch < MinPrefetchInterval)
                return false;
        }

        _recentPrefetches[cacheKey] = now;

        var request = new PrefetchRequest(filePath, offset, length);
        return _prefetchChannel.Writer.TryWrite(request);
    }

    private async Task ProcessPrefetchesAsync(CancellationToken cancellationToken)
    {
        var buffer = BufferPool.Rent(PrefetchBufferSize);
        try
        {
            await foreach (var request in _prefetchChannel.Reader.ReadAllAsync(cancellationToken))
            {
                try
                {
                    await PrefetchAsync(request, buffer, cancellationToken);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    break;
                }
                catch
                {
                    // Prefetch failures are non-critical - continue with next
                }
            }
        }
        finally
        {
            BufferPool.Return(buffer);
        }
    }

    private static async Task PrefetchAsync(PrefetchRequest request, byte[] buffer, CancellationToken cancellationToken)
    {
        if (!System.IO.File.Exists(request.FilePath))
            return;

        try
        {
            // Open with SequentialScan to trigger OS read-ahead
            await using var stream = new FileStream(
                request.FilePath,
                FileMode.Open,
                FileAccess.Read,
                FileShare.Read,
                bufferSize: PrefetchBufferSize,
                options: FileOptions.Asynchronous | FileOptions.SequentialScan);

            if (request.Offset >= stream.Length)
                return;

            stream.Seek(request.Offset, SeekOrigin.Begin);

            var remaining = Math.Min(request.Length, stream.Length - request.Offset);

            // Read in chunks to trigger OS prefetch without consuming too much memory
            while (remaining > 0 && !cancellationToken.IsCancellationRequested)
            {
                var toRead = (int)Math.Min(remaining, buffer.Length);
                var bytesRead = await stream.ReadAsync(buffer.AsMemory(0, toRead), cancellationToken);

                if (bytesRead == 0)
                    break;

                remaining -= bytesRead;
            }
        }
        catch (IOException)
        {
            // File may be locked or unavailable - not critical
        }
    }

    /// <summary>
    /// Cleans up old entries from the recent prefetch tracking.
    /// </summary>
    public void CleanupOldEntries()
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
        _prefetchChannel.Writer.Complete();

        try
        {
            Task.WaitAll(_workerTasks, TimeSpan.FromSeconds(5));
        }
        catch
        {
            // Ignore shutdown errors
        }

        _cts.Dispose();
    }

    private readonly struct PrefetchRequest
    {
        public string FilePath { get; }
        public long Offset { get; }
        public long Length { get; }

        public PrefetchRequest(string filePath, long offset, long length)
        {
            FilePath = filePath;
            Offset = offset;
            Length = length;
        }
    }
}
