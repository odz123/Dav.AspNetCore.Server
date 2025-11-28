using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Text;

namespace Dav.AspNetCore.Server.Performance;

/// <summary>
/// Provides string interning and pooling to reduce allocations for frequently used strings.
/// </summary>
internal static class StringPool
{
    private static readonly ConcurrentDictionary<string, string> Pool = new();
    private const int MaxPoolSize = 10000;

    /// <summary>
    /// Gets a pooled instance of the string, or adds it to the pool if not present.
    /// </summary>
    /// <param name="value">The string to pool.</param>
    /// <returns>A pooled string reference.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static string Intern(string value)
    {
        if (string.IsNullOrEmpty(value))
            return value;

        // Try to get existing pooled string
        if (Pool.TryGetValue(value, out var pooled))
            return pooled;

        // Only add if pool isn't too large
        if (Pool.Count < MaxPoolSize)
        {
            // Use GetOrAdd to handle concurrent additions
            return Pool.GetOrAdd(value, value);
        }

        return value;
    }

    /// <summary>
    /// Interns multiple strings efficiently.
    /// </summary>
    public static IEnumerable<string> InternMany(IEnumerable<string> values)
    {
        foreach (var value in values)
        {
            yield return Intern(value);
        }
    }

    /// <summary>
    /// Clears the string pool.
    /// </summary>
    public static void Clear()
    {
        Pool.Clear();
    }

    /// <summary>
    /// Gets the current size of the pool.
    /// </summary>
    public static int Count => Pool.Count;
}

/// <summary>
/// Provides efficient string building with pooled StringBuilder instances.
/// </summary>
internal static class StringBuilderPool
{
    private static readonly ConcurrentBag<StringBuilder> Pool = new();
    private const int MaxPoolSize = 32;
    private const int DefaultCapacity = 256;

    /// <summary>
    /// Rents a StringBuilder from the pool.
    /// </summary>
    /// <returns>A StringBuilder instance.</returns>
    public static StringBuilder Rent()
    {
        if (Pool.TryTake(out var sb))
        {
            sb.Clear();
            return sb;
        }

        return new StringBuilder(DefaultCapacity);
    }

    /// <summary>
    /// Returns a StringBuilder to the pool.
    /// </summary>
    /// <param name="sb">The StringBuilder to return.</param>
    public static void Return(StringBuilder sb)
    {
        if (sb.Capacity <= 4096 && Pool.Count < MaxPoolSize)
        {
            Pool.Add(sb);
        }
    }

    /// <summary>
    /// Rents a StringBuilder, performs an action, and returns it.
    /// </summary>
    public static string BuildString(Action<StringBuilder> action)
    {
        var sb = Rent();
        try
        {
            action(sb);
            return sb.ToString();
        }
        finally
        {
            Return(sb);
        }
    }
}
