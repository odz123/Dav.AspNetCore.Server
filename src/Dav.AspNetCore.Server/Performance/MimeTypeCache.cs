using System.Collections.Concurrent;
using Microsoft.AspNetCore.StaticFiles;

namespace Dav.AspNetCore.Server.Performance;

/// <summary>
/// Provides cached MIME type lookups using a singleton provider.
/// </summary>
internal static class MimeTypeCache
{
    private static readonly FileExtensionContentTypeProvider Provider = new();
    private static readonly ConcurrentDictionary<string, string> ExtensionCache = new(StringComparer.OrdinalIgnoreCase);
    private const string DefaultMimeType = "application/octet-stream";

    /// <summary>
    /// Gets the MIME type for a file path or URI, using cached lookups.
    /// </summary>
    /// <param name="path">The file path or URI path.</param>
    /// <returns>The MIME type string.</returns>
    public static string GetMimeType(string path)
    {
        // Extract extension
        var lastDot = path.LastIndexOf('.');
        if (lastDot < 0)
            return DefaultMimeType;

        var extension = path[lastDot..].ToLowerInvariant();

        return ExtensionCache.GetOrAdd(extension, ext =>
        {
            if (Provider.TryGetContentType($"file{ext}", out var contentType))
                return contentType;
            return DefaultMimeType;
        });
    }

    /// <summary>
    /// Gets the MIME type for a URI.
    /// </summary>
    /// <param name="uri">The URI.</param>
    /// <returns>The MIME type string.</returns>
    public static string GetMimeType(Uri uri)
    {
        return GetMimeType(uri.AbsolutePath);
    }

    /// <summary>
    /// Tries to get the MIME type for a file path.
    /// </summary>
    /// <param name="path">The file path.</param>
    /// <param name="contentType">The content type if found.</param>
    /// <returns>True if a specific MIME type was found, false if default was used.</returns>
    public static bool TryGetMimeType(string path, out string contentType)
    {
        contentType = GetMimeType(path);
        return contentType != DefaultMimeType;
    }
}
