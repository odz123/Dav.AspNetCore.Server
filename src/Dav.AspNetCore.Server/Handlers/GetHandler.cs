using System.Net;
using System.Xml.Linq;
using Dav.AspNetCore.Server.Http;
using Dav.AspNetCore.Server.Performance;
using Dav.AspNetCore.Server.Store;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Features;

namespace Dav.AspNetCore.Server.Handlers;

internal class GetHandler : RequestHandler
{
    /// <summary>
    /// Handles the web dav request async.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns></returns>
    protected override async Task HandleRequestAsync(CancellationToken cancellationToken = default)
    {
        if (Item == null)
        {
            Context.SetResult(DavStatusCode.NotFound);
            return;
        }

        // Fetch all properties in parallel for better performance
        var properties = await GetPropertiesParallelAsync(Item, cancellationToken);

        // Set response headers
        if (!string.IsNullOrWhiteSpace(properties.ContentType))
            Context.Response.Headers["Content-Type"] = properties.ContentType;

        if (!string.IsNullOrWhiteSpace(properties.ContentLanguage))
            Context.Response.Headers["Content-Language"] = properties.ContentLanguage;

        if (!string.IsNullOrWhiteSpace(properties.LastModified))
            Context.Response.Headers["Last-Modified"] = properties.LastModified;

        if (!string.IsNullOrWhiteSpace(properties.ETag))
            Context.Response.Headers["ETag"] = $"\"{properties.ETag}\"";

        if (!string.IsNullOrWhiteSpace(properties.ContentLength))
            Context.Response.Headers["Content-Length"] = properties.ContentLength;

        // Add Content-Disposition header for file downloads
        var fileName = GetFileName(Item.Uri);
        if (!string.IsNullOrWhiteSpace(fileName))
        {
            var encodedFileName = WebUtility.UrlEncode(fileName);
            Context.Response.Headers["Content-Disposition"] = $"inline; filename=\"{fileName}\"; filename*=UTF-8''{encodedFileName}";
        }

        // Parse content length for buffer optimization
        var contentLength = long.TryParse(properties.ContentLength, out var length) ? length : -1L;

        await using var readableStream = await Item.GetReadableStreamAsync(cancellationToken);
        if (readableStream.CanSeek)
        {
            Context.Response.Headers["Accept-Ranges"] = "bytes";
            Context.Response.ContentLength = readableStream.Length;
            contentLength = readableStream.Length;
        }
        else
        {
            Context.Response.Headers["Accept-Ranges"] = "none";
        }

        // Add streaming-optimized headers
        AddStreamingHeaders(Context, properties.ContentType, contentLength);

        if (Context.Request.Method == WebDavMethods.Head)
        {
            Context.SetResult(DavStatusCode.Ok);
            return;
        }

        var disableRanges = false;

        var requestHeaders = Context.Request.GetTypedHeaders();
        if (requestHeaders.IfRange != null)
        {
            if (requestHeaders.IfRange.EntityTag != null &&
                !string.IsNullOrWhiteSpace(properties.ETag) &&
                requestHeaders.IfRange.EntityTag.Tag != $"\"{properties.ETag}\"")
            {
                disableRanges = true;
            }

            if (requestHeaders.IfRange.LastModified != null &&
                !string.IsNullOrWhiteSpace(properties.LastModified) &&
                DateTimeOffset.TryParse(properties.LastModified, out var parsedLastModified) &&
                requestHeaders.IfRange.LastModified != parsedLastModified)
            {
                disableRanges = true;
            }
        }

        // Try SendFile optimization for physical files (zero-copy transfer)
        var physicalFile = Item as IPhysicalFileInfo;
        var physicalPath = physicalFile?.PhysicalPath;

        if (!string.IsNullOrEmpty(physicalPath) && !disableRanges)
        {
            await SendFileOptimizedAsync(Context, physicalPath, readableStream, contentLength, cancellationToken);
        }
        else
        {
            await SendDataAsync(Context, readableStream, disableRanges, contentLength, cancellationToken);
        }
    }

    /// <summary>
    /// Sends file data using SendFileAsync for zero-copy transfers when possible.
    /// Falls back to stream-based transfer for range requests.
    /// </summary>
    private static async Task SendFileOptimizedAsync(
        HttpContext context,
        string physicalPath,
        Stream stream,
        long contentLength,
        CancellationToken cancellationToken)
    {
        var requestHeaders = context.Request.GetTypedHeaders();
        var responseBodyFeature = context.Features.Get<IHttpResponseBodyFeature>();

        // Handle range requests - still need to use stream for these
        if (requestHeaders.Range != null &&
            requestHeaders.Range.Unit.Equals("bytes") &&
            requestHeaders.Range.Ranges.Count == 1 &&
            stream.CanSeek)
        {
            var range = requestHeaders.Range.Ranges.First();
            var streamLength = stream.Length;

            // Calculate range parameters
            long offset = 0;
            long length = streamLength;

            // Suffix range: last N bytes (e.g., "bytes=-500")
            if (range.From == null && range.To != null)
            {
                var suffixLength = Math.Min(range.To.Value, streamLength);
                offset = streamLength - suffixLength;
                length = suffixLength;
            }
            // Open-ended range: from byte N to end (e.g., "bytes=500-")
            else if (range.From != null && range.To == null)
            {
                if (range.From.Value >= streamLength)
                {
                    context.SetResult(DavStatusCode.RequestedRangeNotSatisfiable);
                    context.Response.Headers["Content-Range"] = $"bytes */{streamLength}";
                    return;
                }
                offset = range.From.Value;
                length = streamLength - offset;
            }
            // Bounded range (e.g., "bytes=500-999")
            else if (range.From != null && range.To != null)
            {
                if (range.From.Value > range.To.Value || range.From.Value >= streamLength)
                {
                    context.SetResult(DavStatusCode.RequestedRangeNotSatisfiable);
                    context.Response.Headers["Content-Range"] = $"bytes */{streamLength}";
                    return;
                }
                offset = range.From.Value;
                var effectiveTo = Math.Min(range.To.Value, streamLength - 1);
                length = effectiveTo - offset + 1;
            }

            context.SetResult(DavStatusCode.PartialContent);
            context.Response.ContentLength = length;
            context.Response.Headers["Content-Range"] = $"bytes {offset}-{offset + length - 1}/{streamLength}";

            // Use SendFileAsync for partial content (zero-copy)
            if (responseBodyFeature != null)
            {
                await responseBodyFeature.SendFileAsync(physicalPath, offset, length, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                // Fallback to stream-based transfer
                stream.Seek(offset, SeekOrigin.Begin);
                var bufferSize = BufferPool.GetOptimalBufferSize(length);
                await stream.CopyToPooledAsync(context.Response.Body, length, bufferSize, cancellationToken).ConfigureAwait(false);
            }
            return;
        }

        // Full file transfer using SendFileAsync (zero-copy)
        context.SetResult(DavStatusCode.Ok);
        if (responseBodyFeature != null)
        {
            await responseBodyFeature.SendFileAsync(physicalPath, 0, contentLength > 0 ? contentLength : null, cancellationToken).ConfigureAwait(false);
        }
        else
        {
            // Fallback to stream-based transfer
            var bufferSize = BufferPool.GetOptimalBufferSize(contentLength);
            await stream.CopyToPooledAsync(context.Response.Body, bufferSize, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Cached property results for a single request.
    /// </summary>
    private readonly record struct FilePropertySet(
        string? ContentType,
        string? ContentLanguage,
        string? LastModified,
        string? ETag,
        string? ContentLength);

    /// <summary>
    /// Fetches all properties in parallel for better performance.
    /// </summary>
    private async Task<FilePropertySet> GetPropertiesParallelAsync(
        IStoreItem item,
        CancellationToken cancellationToken)
    {
        // Run all property fetches in parallel
        var contentTypeTask = GetNonExpensivePropertyAsync(item, XmlNames.GetContentType, cancellationToken);
        var contentLanguageTask = GetNonExpensivePropertyAsync(item, XmlNames.GetContentLanguage, cancellationToken);
        var lastModifiedTask = GetNonExpensivePropertyAsync(item, XmlNames.GetLastModified, cancellationToken);
        var etagTask = GetNonExpensivePropertyAsync(item, XmlNames.GetEtag, cancellationToken);
        var contentLengthTask = GetNonExpensivePropertyAsync(item, XmlNames.GetContentLength, cancellationToken);

        await Task.WhenAll(contentTypeTask, contentLanguageTask, lastModifiedTask, etagTask, contentLengthTask);

        return new FilePropertySet(
            await contentTypeTask,
            await contentLanguageTask,
            await lastModifiedTask,
            await etagTask,
            await contentLengthTask);
    }

    private async Task<string?> GetNonExpensivePropertyAsync(
        IStoreItem item,
        XName propertyName,
        CancellationToken cancellationToken = default)
    {
        var metadata = PropertyManager.GetPropertyMetadata(item, propertyName);
        if (metadata == null || metadata.Expensive)
            return null;

        var result = await PropertyManager.GetPropertyAsync(item, propertyName, cancellationToken);
        return (string?)result.Value;
    }

    /// <summary>
    /// Extracts the file name from a URI.
    /// </summary>
    private static string? GetFileName(Uri uri)
    {
        var path = uri.AbsolutePath;
        var lastSlash = path.LastIndexOf('/');
        if (lastSlash >= 0 && lastSlash < path.Length - 1)
            return Uri.UnescapeDataString(path[(lastSlash + 1)..]);
        return null;
    }

    /// <summary>
    /// Adds streaming-optimized headers to the response.
    /// </summary>
    private static void AddStreamingHeaders(HttpContext context, string? contentType, long contentLength)
    {
        // Allow clients to cache streamable content
        if (IsStreamableContent(contentType))
        {
            // Private cache for 1 hour, must revalidate for changes
            context.Response.Headers["Cache-Control"] = "private, max-age=3600, must-revalidate";
        }

        // Disable response buffering for streaming
        var bufferingFeature = context.Features.Get<IHttpResponseBodyFeature>();
        bufferingFeature?.DisableBuffering();

        // For large files, hint that the connection should be kept alive
        if (contentLength > BufferPool.StreamingThreshold)
        {
            context.Response.Headers["Connection"] = "keep-alive";
            context.Response.Headers["Keep-Alive"] = "timeout=120";
        }
    }

    /// <summary>
    /// Determines if content type is streamable (video, audio, etc.).
    /// </summary>
    private static bool IsStreamableContent(string? contentType)
    {
        if (string.IsNullOrEmpty(contentType))
            return false;

        return contentType.StartsWith("video/", StringComparison.OrdinalIgnoreCase) ||
               contentType.StartsWith("audio/", StringComparison.OrdinalIgnoreCase) ||
               contentType.Equals("application/x-nzb", StringComparison.OrdinalIgnoreCase) ||
               contentType.Equals("application/octet-stream", StringComparison.OrdinalIgnoreCase);
    }

    private static async Task SendDataAsync(
        HttpContext context,
        Stream stream,
        bool disableRanges,
        long contentLength,
        CancellationToken cancellationToken = default)
    {
        var requestHeaders = context.Request.GetTypedHeaders();
        var bufferSize = BufferPool.GetOptimalBufferSize(contentLength);

        if (!disableRanges &&
            requestHeaders.Range != null &&
            requestHeaders.Range.Unit.Equals("bytes") &&
            requestHeaders.Range.Ranges.Count == 1 &&
            stream.CanSeek)
        {
            var range = requestHeaders.Range.Ranges.First();

            var bytesToRead = 0L;
            var streamLength = stream.Length;

            // Suffix range: last N bytes (e.g., "bytes=-500")
            if (range.From == null && range.To != null)
            {
                // If suffix length exceeds file size, return entire file
                var suffixLength = Math.Min(range.To.Value, streamLength);
                stream.Seek(-suffixLength, SeekOrigin.End);
                bytesToRead = suffixLength;
            }

            // Open-ended range: from byte N to end (e.g., "bytes=500-")
            if (range.From != null && range.To == null)
            {
                // If start position is beyond file size, range is unsatisfiable
                if (range.From.Value >= streamLength)
                {
                    context.SetResult(DavStatusCode.RequestedRangeNotSatisfiable);
                    context.Response.Headers["Content-Range"] = $"bytes */{streamLength}";
                    return;
                }
                stream.Seek(range.From.Value, SeekOrigin.Begin);
                bytesToRead = streamLength - stream.Position;
            }

            // Bounded range (e.g., "bytes=500-999")
            if (range.From != null && range.To != null)
            {
                // Validate range: From must be <= To
                if (range.From.Value > range.To.Value)
                {
                    context.SetResult(DavStatusCode.RequestedRangeNotSatisfiable);
                    context.Response.Headers["Content-Range"] = $"bytes */{streamLength}";
                    return;
                }

                // If start position is beyond file size, range is unsatisfiable
                if (range.From.Value >= streamLength)
                {
                    context.SetResult(DavStatusCode.RequestedRangeNotSatisfiable);
                    context.Response.Headers["Content-Range"] = $"bytes */{streamLength}";
                    return;
                }

                // Clamp end position to file size - 1
                var effectiveTo = Math.Min(range.To.Value, streamLength - 1);
                stream.Seek(range.From.Value, SeekOrigin.Begin);
                bytesToRead = effectiveTo - range.From.Value + 1;
            }

            context.SetResult(DavStatusCode.PartialContent);

            var rangeStart = stream.Position;
            var rangeEnd = rangeStart + bytesToRead - 1;

            context.Response.ContentLength = bytesToRead;
            context.Response.Headers["Content-Range"] = $"bytes {rangeStart}-{rangeEnd}/{stream.Length}";

            // Use optimal buffer size for range requests
            var rangeBufferSize = BufferPool.GetOptimalBufferSize(bytesToRead);
            await stream.CopyToPooledAsync(context.Response.Body, bytesToRead, rangeBufferSize, cancellationToken).ConfigureAwait(false);

            return;
        }

        context.SetResult(DavStatusCode.Ok);
        // Use optimal buffer size for full file transfers
        await stream.CopyToPooledAsync(context.Response.Body, bufferSize, cancellationToken).ConfigureAwait(false);
    }
}