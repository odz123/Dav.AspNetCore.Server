using Microsoft.AspNetCore.Http;

namespace Dav.AspNetCore.Server;

internal static class PathStringExtensions
{
    /// <summary>
    /// Converts a PathString to a Uri, properly handling URL encoding.
    /// </summary>
    /// <remarks>
    /// The resulting URI stores both encoded and decoded forms:
    /// - Use .LocalPath for decoded path (filesystem operations)
    /// - Use .AbsolutePath for encoded path (HTTP responses)
    /// Special URI characters like # and ? in the path are properly escaped
    /// to prevent them from being interpreted as fragment/query delimiters.
    /// </remarks>
    public static Uri ToUri(this PathString path)
    {
        if (string.IsNullOrWhiteSpace(path))
            return new Uri("/");

        // Get the URI-encoded component and trim trailing slash
        var encodedPath = path.ToUriComponent().TrimEnd('/');
        if (string.IsNullOrWhiteSpace(encodedPath))
            return new Uri("/");

        // Decode the path first
        var decodedPath = Uri.UnescapeDataString(encodedPath);

        // Re-encode each path segment properly, escaping special URI characters
        // This ensures characters like # and ? are treated as literal path characters
        // and not as fragment/query delimiters
        var segments = decodedPath.Split('/');
        var properlyEncodedPath = string.Join("/",
            segments.Select(segment => Uri.EscapeDataString(segment)));

        // Ensure path starts with /
        if (!properlyEncodedPath.StartsWith("/"))
            properlyEncodedPath = "/" + properlyEncodedPath;

        return new Uri(properlyEncodedPath);
    }
}