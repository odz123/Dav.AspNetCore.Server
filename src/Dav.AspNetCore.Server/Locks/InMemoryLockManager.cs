using System.Collections.Concurrent;
using System.Xml.Linq;
using Dav.AspNetCore.Server.Store;

namespace Dav.AspNetCore.Server.Locks;

public sealed class InMemoryLockManager : ILockManager
{
    // Main lock storage by lock ID
    private readonly ConcurrentDictionary<Uri, ResourceLock> _locksById = new();

    // Index for O(1) lookup by resource URI - maps URI path to lock IDs
    private readonly ConcurrentDictionary<string, ConcurrentDictionary<Uri, byte>> _locksByPath = new();

    // Timer for periodic cleanup of expired locks
    private readonly Timer? _cleanupTimer;
    private readonly TimeSpan _cleanupInterval = TimeSpan.FromMinutes(5);
    private int _cleanupRunning;

    private static readonly IReadOnlyCollection<LockType> SupportedLockTypes = new List<LockType>
    {
        LockType.Exclusive,
        LockType.Shared
    }.AsReadOnly();

    private static readonly ValueTask<IReadOnlyCollection<LockType>> SupportedLocksTask = new(SupportedLockTypes);

    /// <summary>
    /// Initializes a new <see cref="InMemoryLockManager"/> class.
    /// </summary>
    /// <param name="locks">The pre population locks.</param>
    public InMemoryLockManager(IEnumerable<ResourceLock> locks)
    {
        ArgumentNullException.ThrowIfNull(locks, nameof(locks));
        foreach (var resourceLock in locks)
        {
            AddLockToIndexes(resourceLock);
        }

        // Start cleanup timer
        _cleanupTimer = new Timer(CleanupExpiredLocks, null, _cleanupInterval, _cleanupInterval);
    }

    /// <summary>
    /// Gets all held locks.
    /// </summary>
    public IReadOnlyCollection<ResourceLock> Locks
    {
        get
        {
            // Filter out expired locks when accessed
            var activeLocks = new List<ResourceLock>();
            foreach (var kvp in _locksById)
            {
                if (kvp.Value.IsActive)
                    activeLocks.Add(kvp.Value);
            }
            return activeLocks.AsReadOnly();
        }
    }

    private void AddLockToIndexes(ResourceLock resourceLock)
    {
        _locksById[resourceLock.Id] = resourceLock;

        // Add to path index for fast lookup
        var pathKey = resourceLock.Uri.LocalPath;
        var pathLocks = _locksByPath.GetOrAdd(pathKey, _ => new ConcurrentDictionary<Uri, byte>());
        pathLocks[resourceLock.Id] = 0;
    }

    private void RemoveLockFromIndexes(ResourceLock resourceLock)
    {
        _locksById.TryRemove(resourceLock.Id, out _);

        var pathKey = resourceLock.Uri.LocalPath;
        if (_locksByPath.TryGetValue(pathKey, out var pathLocks))
        {
            pathLocks.TryRemove(resourceLock.Id, out _);
            if (pathLocks.IsEmpty)
                _locksByPath.TryRemove(pathKey, out _);
        }
    }

    private void CleanupExpiredLocks(object? state)
    {
        // Prevent concurrent cleanup runs
        if (Interlocked.CompareExchange(ref _cleanupRunning, 1, 0) != 0)
            return;

        try
        {
            var expiredLocks = new List<ResourceLock>();
            foreach (var kvp in _locksById)
            {
                if (!kvp.Value.IsActive)
                    expiredLocks.Add(kvp.Value);
            }

            foreach (var expiredLock in expiredLocks)
            {
                RemoveLockFromIndexes(expiredLock);
            }
        }
        finally
        {
            Interlocked.Exchange(ref _cleanupRunning, 0);
        }
    }

    /// <summary>
    /// Locks the resource async.
    /// </summary>
    /// <param name="uri">The uri.</param>
    /// <param name="lockType">The lock type.</param>
    /// <param name="owner">The lock owner.</param>
    /// <param name="recursive">A value indicating whether the lock will be recursive.</param>
    /// <param name="timeout">The lock timeout.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The lock result.</returns>
    public async ValueTask<LockResult> LockAsync(
        Uri uri,
        LockType lockType,
        XElement owner,
        bool recursive,
        TimeSpan timeout,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(uri, nameof(uri));
        ArgumentNullException.ThrowIfNull(owner, nameof(owner));

        var activeLocks = await GetLocksAsync(uri, cancellationToken).ConfigureAwait(false);
        if ((activeLocks.All(x => x.LockType == LockType.Shared) &&
             lockType == LockType.Shared) ||
            activeLocks.Count == 0)
        {
            var newLock = new ResourceLock(
                new Uri($"urn:uuid:{Guid.NewGuid():D}"),
                uri,
                lockType,
                owner,
                recursive,
                timeout,
                DateTime.UtcNow);

            AddLockToIndexes(newLock);
            return new LockResult(DavStatusCode.Ok, newLock);
        }

        return new LockResult(DavStatusCode.Locked);
    }

    /// <summary>
    /// Refreshes the resource lock async.
    /// </summary>
    /// <param name="uri">The uri.</param>
    /// <param name="token">The lock token.</param>
    /// <param name="timeout">The lock timeout.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The lock result.</returns>
    public ValueTask<LockResult> RefreshLockAsync(
        Uri uri,
        Uri token,
        TimeSpan timeout,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(uri, nameof(uri));
        ArgumentNullException.ThrowIfNull(token, nameof(token));

        // O(1) lookup by token ID
        if (!_locksById.TryGetValue(token, out var activeLock) ||
            activeLock.Uri != uri ||
            !activeLock.IsActive)
        {
            return new ValueTask<LockResult>(new LockResult(DavStatusCode.PreconditionFailed));
        }

        var refreshLock = activeLock with
        {
            Timeout = timeout,
            IssueDate = DateTime.UtcNow
        };

        // Update the lock in place
        _locksById[refreshLock.Id] = refreshLock;

        return new ValueTask<LockResult>(new LockResult(DavStatusCode.Ok, refreshLock));
    }

    /// <summary>
    /// Unlocks the resource async.
    /// </summary>
    /// <param name="uri">The uri.</param>
    /// <param name="token">The lock token.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The status code.</returns>
    public ValueTask<DavStatusCode> UnlockAsync(
        Uri uri,
        Uri token,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(uri, nameof(uri));
        ArgumentNullException.ThrowIfNull(token, nameof(token));

        // O(1) lookup by token ID
        if (!_locksById.TryGetValue(token, out var activeLock) ||
            activeLock.Uri != uri ||
            !activeLock.IsActive)
        {
            return new ValueTask<DavStatusCode>(DavStatusCode.Conflict);
        }

        RemoveLockFromIndexes(activeLock);
        return new ValueTask<DavStatusCode>(DavStatusCode.NoContent);
    }

    /// <summary>
    /// Gets all active resource locks async.
    /// </summary>
    /// <param name="uri">The uri.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A list of all active resource locks for the given store item.</returns>
    public ValueTask<IReadOnlyCollection<ResourceLock>> GetLocksAsync(
        Uri uri,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(uri, nameof(uri));

        var allActiveLocks = new List<ResourceLock>();
        var localPath = uri.LocalPath;

        // Build path segments for hierarchical lookup
        // For "/a/b/c", check: "/", "/a", "/a/b", "/a/b/c"
        var pathsToCheck = new List<string>();

        if (localPath == "/")
        {
            pathsToCheck.Add("/");
        }
        else
        {
            var segments = localPath.Split('/', StringSplitOptions.RemoveEmptyEntries);
            var currentPath = string.Empty;

            // Add root path
            pathsToCheck.Add("/");

            for (var i = 0; i < segments.Length; i++)
            {
                currentPath = $"{currentPath}/{segments[i]}";
                pathsToCheck.Add(currentPath);
            }
        }

        var isTargetPath = pathsToCheck.Count > 0;
        var targetPathIndex = pathsToCheck.Count - 1;

        for (var i = 0; i < pathsToCheck.Count; i++)
        {
            var pathToCheck = pathsToCheck[i];
            var isTarget = i == targetPathIndex;

            // O(1) lookup in path index
            if (_locksByPath.TryGetValue(pathToCheck, out var lockIds))
            {
                foreach (var lockId in lockIds.Keys)
                {
                    if (_locksById.TryGetValue(lockId, out var resourceLock) && resourceLock.IsActive)
                    {
                        // Include lock if:
                        // 1. It's on the target path (exact match), OR
                        // 2. It's on a parent path AND is recursive
                        if (isTarget || resourceLock.Recursive)
                        {
                            allActiveLocks.Add(resourceLock);
                        }
                    }
                }
            }
        }

        return ValueTask.FromResult<IReadOnlyCollection<ResourceLock>>(allActiveLocks);
    }

    /// <summary>
    /// Gets the supported locks async.
    /// </summary>
    /// <param name="item">The store item.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A list of available lock types for the given resource.</returns>
    public ValueTask<IReadOnlyCollection<LockType>> GetSupportedLocksAsync(
        IStoreItem item,
        CancellationToken cancellationToken = default)
        => SupportedLocksTask;
}