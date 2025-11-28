using System.Collections.Concurrent;

namespace Dav.AspNetCore.Server.Performance;

/// <summary>
/// A thread-safe LRU (Least Recently Used) cache with bounded capacity.
/// </summary>
/// <typeparam name="TKey">The type of the cache key.</typeparam>
/// <typeparam name="TValue">The type of the cached value.</typeparam>
internal sealed class LruCache<TKey, TValue> where TKey : notnull
{
    private readonly int _capacity;
    private readonly ConcurrentDictionary<TKey, LinkedListNode<CacheEntry>> _cache;
    private readonly LinkedList<CacheEntry> _lruList;
    private readonly object _lock = new();

    private sealed class CacheEntry
    {
        public TKey Key { get; }
        public TValue Value { get; set; }

        public CacheEntry(TKey key, TValue value)
        {
            Key = key;
            Value = value;
        }
    }

    /// <summary>
    /// Initializes a new instance of the LruCache class.
    /// </summary>
    /// <param name="capacity">The maximum number of items in the cache.</param>
    public LruCache(int capacity)
    {
        if (capacity <= 0)
            throw new ArgumentOutOfRangeException(nameof(capacity), "Capacity must be positive.");

        _capacity = capacity;
        _cache = new ConcurrentDictionary<TKey, LinkedListNode<CacheEntry>>();
        _lruList = new LinkedList<CacheEntry>();
    }

    /// <summary>
    /// Gets the current count of items in the cache.
    /// </summary>
    public int Count => _cache.Count;

    /// <summary>
    /// Tries to get a value from the cache.
    /// </summary>
    /// <param name="key">The key to look up.</param>
    /// <param name="value">The value if found.</param>
    /// <returns>True if the key was found, false otherwise.</returns>
    public bool TryGetValue(TKey key, out TValue? value)
    {
        if (_cache.TryGetValue(key, out var node))
        {
            lock (_lock)
            {
                // Move to front (most recently used)
                if (node.List != null)
                {
                    _lruList.Remove(node);
                    _lruList.AddFirst(node);
                }
            }
            value = node.Value.Value;
            return true;
        }

        value = default;
        return false;
    }

    /// <summary>
    /// Adds or updates a value in the cache.
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="value">The value.</param>
    public void Set(TKey key, TValue value)
    {
        lock (_lock)
        {
            if (_cache.TryGetValue(key, out var existingNode))
            {
                // Update existing entry
                existingNode.Value.Value = value;
                _lruList.Remove(existingNode);
                _lruList.AddFirst(existingNode);
            }
            else
            {
                // Add new entry
                var entry = new CacheEntry(key, value);
                var node = new LinkedListNode<CacheEntry>(entry);

                _cache[key] = node;
                _lruList.AddFirst(node);

                // Evict if over capacity
                while (_cache.Count > _capacity && _lruList.Last != null)
                {
                    var lastNode = _lruList.Last;
                    _lruList.RemoveLast();
                    _cache.TryRemove(lastNode.Value.Key, out _);
                }
            }
        }
    }

    /// <summary>
    /// Gets or adds a value to the cache.
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="valueFactory">The factory to create the value if not present.</param>
    /// <returns>The existing or newly created value.</returns>
    public TValue GetOrAdd(TKey key, Func<TKey, TValue> valueFactory)
    {
        if (TryGetValue(key, out var value))
            return value!;

        var newValue = valueFactory(key);
        Set(key, newValue);
        return newValue;
    }

    /// <summary>
    /// Gets or adds a value to the cache asynchronously.
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="valueFactory">The async factory to create the value if not present.</param>
    /// <returns>The existing or newly created value.</returns>
    public async ValueTask<TValue> GetOrAddAsync(TKey key, Func<TKey, ValueTask<TValue>> valueFactory)
    {
        if (TryGetValue(key, out var value))
            return value!;

        var newValue = await valueFactory(key).ConfigureAwait(false);
        Set(key, newValue);
        return newValue;
    }

    /// <summary>
    /// Removes a specific key from the cache.
    /// </summary>
    /// <param name="key">The key to remove.</param>
    /// <returns>True if the key was removed, false if it wasn't found.</returns>
    public bool TryRemove(TKey key, out TValue? value)
    {
        lock (_lock)
        {
            if (_cache.TryRemove(key, out var node))
            {
                _lruList.Remove(node);
                value = node.Value.Value;
                return true;
            }
        }

        value = default;
        return false;
    }

    /// <summary>
    /// Clears all entries from the cache.
    /// </summary>
    public void Clear()
    {
        lock (_lock)
        {
            _cache.Clear();
            _lruList.Clear();
        }
    }

    /// <summary>
    /// Indexer for getting and setting values.
    /// </summary>
    public TValue this[TKey key]
    {
        get
        {
            if (TryGetValue(key, out var value))
                return value!;
            throw new KeyNotFoundException($"The key '{key}' was not found in the cache.");
        }
        set => Set(key, value);
    }

    /// <summary>
    /// Checks if the cache contains the specified key.
    /// </summary>
    public bool ContainsKey(TKey key) => _cache.ContainsKey(key);
}
