namespace Dav.AspNetCore.Server;

internal static class UriHelper
{
    public static Uri GetParent(this Uri uri)
    {
        ArgumentNullException.ThrowIfNull(uri, nameof(uri));

        if (uri.Segments.Length == 1)
            return new Uri(uri.Segments[0]);

        var uriString = string.Empty;
        for (var i = 0; i < uri.Segments.Length - 1; i++)
        {
            uriString += uri.Segments[i];
        }

        return new Uri(uriString);
    }

    public static Uri Combine(Uri uri, string path)
    {
        // Use AbsolutePath (encoded) to preserve proper encoding
        var encodedBasePath = uri.AbsolutePath;
        if (!encodedBasePath.EndsWith("/"))
            encodedBasePath += "/";

        // The path parameter is typically a decoded item name, so we need to encode it
        // to prevent special characters like # from being interpreted as URI delimiters
        var encodedPath = Uri.EscapeDataString(path.TrimStart('/'));

        return new Uri($"{encodedBasePath}{encodedPath}");
    }

    public static Uri GetRelativeUri(this Uri relativeTo, Uri uri)
    {
        ArgumentNullException.ThrowIfNull(relativeTo, nameof(relativeTo));
        ArgumentNullException.ThrowIfNull(uri, nameof(uri));
        
        if (uri.Segments.Length > relativeTo.Segments.Length)
            return uri;
        
        // validate root
        for (var i = 0; i < uri.Segments.Length; i++)
        {
            if (relativeTo.Segments[i].Trim('/') != uri.Segments[i].Trim('/'))
                return uri;
        }

        var relativePath = string.Join("", relativeTo.Segments.Skip(uri.Segments.Length));
        if (!relativePath.StartsWith("/"))
            relativePath = $"/{relativePath}";

        if (relativePath.EndsWith("/"))
            relativePath = relativePath.TrimEnd('/');

        if (string.IsNullOrWhiteSpace(relativePath))
            return new Uri("/");
        
        return new Uri(relativePath);
    }
}