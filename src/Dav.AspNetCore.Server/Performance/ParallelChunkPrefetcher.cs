using System.Collections.Concurrent;
using System.Threading.Channels;

namespace Dav.AspNetCore.Server.Performance;

/// <summary>
/// Prefetches initial chunks of files in parallel for faster stream starts.
/// When a stream starts, this loads multiple initial chunks concurrently
/// so they're ready when the client requests them.
/// </summary>
internal sealed class ParallelChunkPrefetcher : IDisposable
{
    private static readonly Lazy<ParallelChunkPrefetcher> LazyInstance = new(() => new ParallelChunkPrefetcher());
    public static ParallelChunkPrefetcher Instance => LazyInstance.Value;

    /// <summary>
    /// Size of each chunk to prefetch (1MB).
    /// </summary>
    public const int ChunkSize = 1024 * 1024;

    /// <summary>
    /// Number of initial chunks to prefetch in parallel.
    /// </summary>
    public const int InitialChunkCount = 4;

    /// <summary>
    /// Maximum concurrent prefetch operations.
    /// </summary>
    private const int MaxConcurrentPrefetches = 8;

    /// <summary>
    /// Maximum files to track.
    /// </summary>
    private const int MaxTrackedFiles = 500;

    /// <summary>
    /// How long to keep prefetched data.
    /// </summary>
    private static readonly TimeSpan PrefetchDataExpiration = TimeSpan.FromSeconds(30);

    private readonly Channel<PrefetchRequest> _requestChannel;
    private readonly LruCache<string, PrefetchedFile> _prefetchedFiles;
    private readonly SemaphoreSlim _concurrencySemaphore;
    private readonly Task[] _workerTasks;
    private readonly CancellationTokenSource _cts;
    private bool _disposed;

    private ParallelChunkPrefetcher()
    {
        _requestChannel = Channel.CreateBounded<PrefetchRequest>(new BoundedChannelOptions(100)
        {
            FullMode = BoundedChannelFullMode.DropOldest,
            SingleReader = false,
            SingleWriter = false
        });

        _prefetchedFiles = new LruCache<string, PrefetchedFile>(MaxTrackedFiles);
        _concurrencySemaphore = new SemaphoreSlim(MaxConcurrentPrefetches, MaxConcurrentPrefetches);
        _cts = new CancellationTokenSource();

        // Start worker tasks
        _workerTasks = new Task[4];
        for (int i = 0; i < _workerTasks.Length; i++)
        {
            _workerTasks[i] = Task.Run(() => ProcessRequestsAsync(_cts.Token));
        }
    }

    /// <summary>
    /// Triggers parallel prefetch of initial file chunks.
    /// Call this when a new stream is detected.
    /// </summary>
    /// <param name="filePath">The physical file path.</param>
    /// <param name="fileSize">The file size.</param>
    public void TriggerInitialPrefetch(string filePath, long fileSize)
    {
        if (_disposed || string.IsNullOrEmpty(filePath))
            return;

        // Check if already prefetched
        if (_prefetchedFiles.TryGetValue(filePath, out var existing) &&
            existing != null && !existing.IsExpired)
        {
            return;
        }

        // Queue the prefetch request
        var request = new PrefetchRequest(filePath, fileSize, 0, InitialChunkCount);
        _requestChannel.Writer.TryWrite(request);
    }

    /// <summary>
    /// Triggers prefetch for a specific range (for seek operations).
    /// </summary>
    /// <param name="filePath">The physical file path.</param>
    /// <param name="fileSize">The file size.</param>
    /// <param name="startOffset">The starting offset.</param>
    /// <param name="chunkCount">Number of chunks to prefetch.</param>
    public void TriggerRangePrefetch(string filePath, long fileSize, long startOffset, int chunkCount = 2)
    {
        if (_disposed || string.IsNullOrEmpty(filePath))
            return;

        var request = new PrefetchRequest(filePath, fileSize, startOffset, chunkCount);
        _requestChannel.Writer.TryWrite(request);
    }

    /// <summary>
    /// Gets a prefetched chunk if available.
    /// </summary>
    /// <param name="filePath">The file path.</param>
    /// <param name="offset">The chunk offset.</param>
    /// <param name="data">The prefetched data.</param>
    /// <returns>True if data was available.</returns>
    public bool TryGetPrefetchedChunk(string filePath, long offset, out ReadOnlyMemory<byte> data)
    {
        data = default;

        if (_prefetchedFiles.TryGetValue(filePath, out var prefetchedFile) &&
            prefetchedFile != null && !prefetchedFile.IsExpired)
        {
            return prefetchedFile.TryGetChunk(offset, out data);
        }

        return false;
    }

    /// <summary>
    /// Checks if a chunk is being prefetched.
    /// </summary>
    public bool IsChunkBeingPrefetched(string filePath, long offset)
    {
        if (_prefetchedFiles.TryGetValue(filePath, out var prefetchedFile) &&
            prefetchedFile != null)
        {
            return prefetchedFile.IsChunkPending(offset);
        }
        return false;
    }

    /// <summary>
    /// Waits for a specific chunk to be prefetched.
    /// </summary>
    public async ValueTask<ReadOnlyMemory<byte>> WaitForChunkAsync(
        string filePath,
        long offset,
        CancellationToken cancellationToken)
    {
        if (_prefetchedFiles.TryGetValue(filePath, out var prefetchedFile) &&
            prefetchedFile != null)
        {
            return await prefetchedFile.WaitForChunkAsync(offset, cancellationToken);
        }

        return default;
    }

    private async Task ProcessRequestsAsync(CancellationToken cancellationToken)
    {
        await foreach (var request in _requestChannel.Reader.ReadAllAsync(cancellationToken))
        {
            try
            {
                await ProcessPrefetchRequestAsync(request, cancellationToken);
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

    private async Task ProcessPrefetchRequestAsync(PrefetchRequest request, CancellationToken cancellationToken)
    {
        if (!File.Exists(request.FilePath))
            return;

        // Get or create the prefetched file entry
        var prefetchedFile = _prefetchedFiles.GetOrAdd(
            request.FilePath,
            _ => new PrefetchedFile(request.FilePath, request.FileSize));

        // Calculate chunks to prefetch
        var chunkTasks = new List<Task>();
        var startChunk = request.StartOffset / ChunkSize;
        var endChunk = Math.Min(startChunk + request.ChunkCount, (request.FileSize + ChunkSize - 1) / ChunkSize);

        // Use Linux kernel hints if available
        if (LinuxKernelHints.IsAvailable)
        {
            LinuxKernelHints.PrefetchFileRange(
                request.FilePath,
                request.StartOffset,
                (endChunk - startChunk) * ChunkSize);
        }

        for (long chunk = startChunk; chunk < endChunk; chunk++)
        {
            var chunkOffset = chunk * ChunkSize;

            // Skip if already loaded or pending
            if (prefetchedFile.HasChunk(chunkOffset) || prefetchedFile.IsChunkPending(chunkOffset))
                continue;

            // Mark as pending
            prefetchedFile.MarkChunkPending(chunkOffset);

            var capturedOffset = chunkOffset;
            var task = Task.Run(async () =>
            {
                await _concurrencySemaphore.WaitAsync(cancellationToken);
                try
                {
                    await LoadChunkAsync(prefetchedFile, capturedOffset, request.FileSize, cancellationToken);
                }
                finally
                {
                    _concurrencySemaphore.Release();
                }
            }, cancellationToken);

            chunkTasks.Add(task);
        }

        // Wait for all chunks to load
        if (chunkTasks.Count > 0)
        {
            await Task.WhenAll(chunkTasks);
        }
    }

    private static async Task LoadChunkAsync(
        PrefetchedFile prefetchedFile,
        long offset,
        long fileSize,
        CancellationToken cancellationToken)
    {
        try
        {
            var chunkLength = (int)Math.Min(ChunkSize, fileSize - offset);
            if (chunkLength <= 0)
                return;

            var buffer = BufferPool.Rent(chunkLength);
            try
            {
                await using var stream = new FileStream(
                    prefetchedFile.FilePath,
                    FileMode.Open,
                    FileAccess.Read,
                    FileShare.Read,
                    bufferSize: chunkLength,
                    FileOptions.Asynchronous | FileOptions.SequentialScan);

                stream.Seek(offset, SeekOrigin.Begin);

                int totalRead = 0;
                while (totalRead < chunkLength)
                {
                    var read = await stream.ReadAsync(
                        buffer.AsMemory(totalRead, chunkLength - totalRead),
                        cancellationToken);

                    if (read == 0)
                        break;

                    totalRead += read;
                }

                // Copy to a new array for storage (we'll return the rented buffer)
                var data = new byte[totalRead];
                Array.Copy(buffer, data, totalRead);

                prefetchedFile.SetChunk(offset, data);
            }
            finally
            {
                BufferPool.Return(buffer);
            }
        }
        catch
        {
            prefetchedFile.ClearChunkPending(offset);
        }
    }

    /// <summary>
    /// Invalidates prefetched data for a file.
    /// </summary>
    public void Invalidate(string filePath)
    {
        if (_prefetchedFiles.TryRemove(filePath, out var prefetchedFile) && prefetchedFile != null)
        {
            prefetchedFile.Clear();
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

        // Clear all prefetched data
        foreach (var key in _prefetchedFiles.Keys.ToList())
        {
            if (_prefetchedFiles.TryRemove(key, out var file) && file != null)
            {
                file.Clear();
            }
        }
    }

    private readonly struct PrefetchRequest
    {
        public string FilePath { get; }
        public long FileSize { get; }
        public long StartOffset { get; }
        public int ChunkCount { get; }

        public PrefetchRequest(string filePath, long fileSize, long startOffset, int chunkCount)
        {
            FilePath = filePath;
            FileSize = fileSize;
            StartOffset = startOffset;
            ChunkCount = chunkCount;
        }
    }

    /// <summary>
    /// Stores prefetched data for a single file.
    /// </summary>
    private sealed class PrefetchedFile
    {
        public string FilePath { get; }
        public long FileSize { get; }
        private readonly ConcurrentDictionary<long, byte[]> _chunks;
        private readonly ConcurrentDictionary<long, TaskCompletionSource<byte[]>> _pendingChunks;
        private readonly DateTime _createdAt;

        public PrefetchedFile(string filePath, long fileSize)
        {
            FilePath = filePath;
            FileSize = fileSize;
            _chunks = new ConcurrentDictionary<long, byte[]>();
            _pendingChunks = new ConcurrentDictionary<long, TaskCompletionSource<byte[]>>();
            _createdAt = DateTime.UtcNow;
        }

        public bool IsExpired => DateTime.UtcNow - _createdAt > PrefetchDataExpiration;

        public bool HasChunk(long offset) => _chunks.ContainsKey(offset);

        public bool IsChunkPending(long offset) => _pendingChunks.ContainsKey(offset);

        public void MarkChunkPending(long offset)
        {
            _pendingChunks.TryAdd(offset, new TaskCompletionSource<byte[]>());
        }

        public void ClearChunkPending(long offset)
        {
            if (_pendingChunks.TryRemove(offset, out var tcs))
            {
                tcs.TrySetResult(Array.Empty<byte>());
            }
        }

        public void SetChunk(long offset, byte[] data)
        {
            _chunks[offset] = data;

            if (_pendingChunks.TryRemove(offset, out var tcs))
            {
                tcs.TrySetResult(data);
            }
        }

        public bool TryGetChunk(long offset, out ReadOnlyMemory<byte> data)
        {
            if (_chunks.TryGetValue(offset, out var bytes))
            {
                data = bytes;
                return true;
            }

            data = default;
            return false;
        }

        public async ValueTask<ReadOnlyMemory<byte>> WaitForChunkAsync(
            long offset,
            CancellationToken cancellationToken)
        {
            // Check if already loaded
            if (_chunks.TryGetValue(offset, out var bytes))
                return bytes;

            // Check if pending
            if (_pendingChunks.TryGetValue(offset, out var tcs))
            {
                using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                cts.CancelAfter(TimeSpan.FromSeconds(10));

                try
                {
                    return await tcs.Task.WaitAsync(cts.Token);
                }
                catch (OperationCanceledException)
                {
                    return default;
                }
            }

            return default;
        }

        public void Clear()
        {
            _chunks.Clear();
            foreach (var tcs in _pendingChunks.Values)
            {
                tcs.TrySetCanceled();
            }
            _pendingChunks.Clear();
        }
    }
}
