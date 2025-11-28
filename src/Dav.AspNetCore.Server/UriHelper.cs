using System.Runtime.CompilerServices;
using System.Text;
using Dav.AspNetCore.Server.Performance;

namespace Dav.AspNetCore.Server;

internal static class UriHelper
{
    // Cache for root URI
    private static readonly Uri RootUri = new("/");

    // LRU cache for parent URIs
    private static readonly LruCache<string, Uri> ParentCache = new(1000);

    // LRU cache for combined URIs
    private static readonly LruCache<(string, string), Uri> CombineCache = new(2000);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Uri GetParent(this Uri uri)
    {
        ArgumentNullException.ThrowIfNull(uri, nameof(uri));

        var cacheKey = uri.AbsolutePath;

        if (ParentCache.TryGetValue(cacheKey, out var cachedParent))
            return cachedParent!;

        var segments = uri.Segments;
        if (segments.Length == 1)
        {
            var result = new Uri(segments[0]);
            ParentCache.Set(cacheKey, result);
            return result;
        }

        // Use StringBuilder for efficient string concatenation
        var sb = StringBuilderPool.Rent();
        try
        {
            for (var i = 0; i < segments.Length - 1; i++)
            {
                sb.Append(segments[i]);
            }

            var parentUri = new Uri(sb.ToString());
            ParentCache.Set(cacheKey, parentUri);
            return parentUri;
        }
        finally
        {
            StringBuilderPool.Return(sb);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Uri Combine(Uri uri, string path)
    {
        var basePath = uri.AbsolutePath;
        var cacheKey = (basePath, path);

        if (CombineCache.TryGetValue(cacheKey, out var cachedUri))
            return cachedUri!;

        // Use AbsolutePath (encoded) to preserve proper encoding
        // The path parameter is typically a decoded item name, so we need to encode it
        // to prevent special characters like # from being interpreted as URI delimiters
        var trimmedPath = path.AsSpan().TrimStart('/');
        var encodedPath = Uri.EscapeDataString(trimmedPath.ToString());

        // Use StringBuilder for efficient concatenation
        var sb = StringBuilderPool.Rent();
        try
        {
            sb.Append(basePath);
            if (!basePath.EndsWith('/'))
                sb.Append('/');
            sb.Append(encodedPath);

            var result = new Uri(sb.ToString());
            CombineCache.Set(cacheKey, result);
            return result;
        }
        finally
        {
            StringBuilderPool.Return(sb);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Uri GetRelativeUri(this Uri relativeTo, Uri uri)
    {
        ArgumentNullException.ThrowIfNull(relativeTo, nameof(relativeTo));
        ArgumentNullException.ThrowIfNull(uri, nameof(uri));

        var relativeToSegments = relativeTo.Segments;
        var uriSegments = uri.Segments;

        if (uriSegments.Length > relativeToSegments.Length)
            return uri;

        // validate root
        for (var i = 0; i < uriSegments.Length; i++)
        {
            var seg1 = relativeToSegments[i].AsSpan().Trim('/');
            var seg2 = uriSegments[i].AsSpan().Trim('/');
            if (!seg1.SequenceEqual(seg2))
                return uri;
        }

        // Build relative path using StringBuilder
        var sb = StringBuilderPool.Rent();
        try
        {
            for (var i = uriSegments.Length; i < relativeToSegments.Length; i++)
            {
                sb.Append(relativeToSegments[i]);
            }

            var relativePath = sb.ToString();

            if (relativePath.Length == 0 || string.IsNullOrWhiteSpace(relativePath))
                return RootUri;

            if (!relativePath.StartsWith('/'))
            {
                sb.Clear();
                sb.Append('/');
                sb.Append(relativePath);
                relativePath = sb.ToString();
            }

            if (relativePath.EndsWith('/'))
                relativePath = relativePath.TrimEnd('/');

            if (string.IsNullOrWhiteSpace(relativePath))
                return RootUri;

            return new Uri(relativePath);
        }
        finally
        {
            StringBuilderPool.Return(sb);
        }
    }
}