using System.Net;
using System.Xml.Linq;
using Dav.AspNetCore.Server.Http;
using Dav.AspNetCore.Server.Performance;
using Dav.AspNetCore.Server.Store;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Features;

namespace Dav.AspNetCore.Server.Handlers;

/// <summary>
/// Handles HTTP GET and HEAD requests for WebDAV resources.
/// Optimized for fast streaming with zero-copy file transfers.
/// </summary>
internal sealed class GetHandler : RequestHandler
{
    /// <summary>
    /// Handles the web dav request async.
    /// </summary>
    protected override async Task HandleRequestAsync(CancellationToken cancellationToken = default)
    {
        if (Item == null)
        {
            Context.SetResult(DavStatusCode.NotFound);
            return;
        }

        // Get request headers
        var requestHeaders = Context.Request.GetTypedHeaders();
        var isHeadRequest = Context.Request.Method == WebDavMethods.Head;

        // Check for range request
        var hasRangeRequest = requestHeaders.Range != null &&
                              requestHeaders.Range.Unit.Equals("bytes") &&
                              requestHeaders.Range.Ranges.Count == 1;

        // Get file info
        var optimizedItem = Item as IOptimizedStreamable;
        var physicalFile = Item as IPhysicalFileInfo;
        var physicalPath = physicalFile?.PhysicalPath;
        var hasPhysicalPath = !string.IsNullOrEmpty(physicalPath);

        // Get content length - prefer cached metadata
        long contentLength;
        if (optimizedItem != null)
        {
            contentLength = optimizedItem.Length;
        }
        else
        {
            var contentLengthResult = await PropertyManager.GetPropertyAsync(Item, XmlNames.GetContentLength, cancellationToken);
            contentLength = long.TryParse(contentLengthResult.Value?.ToString(), out var len) ? len : -1L;
        }

        // Fetch essential properties
        var contentType = await GetPropertyValueAsync(XmlNames.GetContentType, cancellationToken);
        var etag = await GetPropertyValueAsync(XmlNames.GetEtag, cancellationToken);
        var lastModified = await GetPropertyValueAsync(XmlNames.GetLastModified, cancellationToken);

        // Set response headers
        SetResponseHeaders(contentType, etag, lastModified, contentLength);

        // Check If-Range condition
        var disableRanges = CheckIfRangeCondition(requestHeaders, etag, lastModified);

        // Handle HEAD request without opening stream
        if (isHeadRequest)
        {
            Context.Response.Headers["Accept-Ranges"] = hasPhysicalPath || (optimizedItem != null && contentLength > 0)
                ? ResponseHeaderCache.AcceptRangesBytes
                : ResponseHeaderCache.AcceptRangesNone;

            if (contentLength >= 0)
                Context.Response.ContentLength = contentLength;

            Context.SetResult(DavStatusCode.Ok);
            return;
        }

        // Determine access pattern
        var accessPattern = (hasRangeRequest && !disableRanges)
            ? FileAccessPattern.RandomAccess
            : FileAccessPattern.Sequential;

        // For physical files, use zero-copy SendFile
        if (hasPhysicalPath && !disableRanges)
        {
            await SendFileAsync(physicalPath!, contentLength, hasRangeRequest, cancellationToken);
            return;
        }

        // Open stream with optimized access pattern
        Stream readableStream;
        if (optimizedItem != null)
        {
            readableStream = await optimizedItem.GetOptimizedReadableStreamAsync(accessPattern, cancellationToken);
        }
        else
        {
            readableStream = await Item.GetReadableStreamAsync(cancellationToken);
        }

        await using (readableStream)
        {
            if (hasRangeRequest && !disableRanges && readableStream.CanSeek)
            {
                await SendRangeAsync(readableStream, cancellationToken);
            }
            else
            {
                await SendFullContentAsync(readableStream, contentLength, cancellationToken);
            }
        }
    }

    /// <summary>
    /// Sets response headers from file properties.
    /// </summary>
    private void SetResponseHeaders(string? contentType, string? etag, string? lastModified, long contentLength)
    {
        if (!string.IsNullOrWhiteSpace(contentType))
            Context.Response.Headers["Content-Type"] = contentType;

        if (!string.IsNullOrWhiteSpace(lastModified))
            Context.Response.Headers["Last-Modified"] = lastModified;

        if (!string.IsNullOrWhiteSpace(etag))
            Context.Response.Headers["ETag"] = $"\"{etag}\"";

        if (contentLength >= 0)
            Context.Response.Headers["Content-Length"] = contentLength.ToString();

        // Add Content-Disposition header
        var fileName = GetFileName();
        if (!string.IsNullOrWhiteSpace(fileName))
        {
            var encodedFileName = WebUtility.UrlEncode(fileName);
            Context.Response.Headers["Content-Disposition"] = $"inline; filename=\"{fileName}\"; filename*=UTF-8''{encodedFileName}";
        }
    }

    /// <summary>
    /// Checks If-Range precondition.
    /// </summary>
    private static bool CheckIfRangeCondition(
        Microsoft.AspNetCore.Http.Headers.RequestHeaders requestHeaders,
        string? etag,
        string? lastModified)
    {
        if (requestHeaders.IfRange == null)
            return false;

        // Check ETag mismatch
        if (requestHeaders.IfRange.EntityTag != null &&
            !string.IsNullOrWhiteSpace(etag) &&
            requestHeaders.IfRange.EntityTag.Tag != $"\"{etag}\"")
        {
            return true;
        }

        // Check Last-Modified mismatch
        if (requestHeaders.IfRange.LastModified != null &&
            !string.IsNullOrWhiteSpace(lastModified) &&
            DateTimeOffset.TryParse(lastModified, out var parsedLastModified) &&
            requestHeaders.IfRange.LastModified != parsedLastModified)
        {
            return true;
        }

        return false;
    }

    /// <summary>
    /// Sends file using zero-copy SendFileAsync.
    /// </summary>
    private async Task SendFileAsync(
        string physicalPath,
        long contentLength,
        bool hasRangeRequest,
        CancellationToken cancellationToken)
    {
        var requestHeaders = Context.Request.GetTypedHeaders();
        var responseBodyFeature = Context.Features.Get<IHttpResponseBodyFeature>();

        Context.Response.Headers["Accept-Ranges"] = ResponseHeaderCache.AcceptRangesBytes;

        // Handle range request
        if (hasRangeRequest)
        {
            var range = requestHeaders.Range!.Ranges.First();

            if (!TryCalculateRange(range, contentLength, out var offset, out var length))
            {
                Context.SetResult(DavStatusCode.RequestedRangeNotSatisfiable);
                Context.Response.Headers["Content-Range"] = $"bytes */{contentLength}";
                return;
            }

            Context.SetResult(DavStatusCode.PartialContent);
            Context.Response.ContentLength = length;
            Context.Response.Headers["Content-Range"] = ResponseHeaderCache.GetContentRangeHeader(offset, offset + length - 1, contentLength);

            // Disable buffering and flush headers
            responseBodyFeature?.DisableBuffering();
            await Context.Response.StartAsync(cancellationToken).ConfigureAwait(false);

            if (responseBodyFeature != null)
            {
                await responseBodyFeature.SendFileAsync(physicalPath, offset, length, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                // Fallback to stream
                await using var stream = new FileStream(physicalPath, FileMode.Open, FileAccess.Read, FileShare.Read,
                    BufferPool.DefaultBufferSize, FileOptions.Asynchronous | FileOptions.RandomAccess);
                stream.Seek(offset, SeekOrigin.Begin);
                await stream.CopyToPooledAsync(Context.Response.Body, length, BufferPool.GetOptimalBufferSize(length), cancellationToken).ConfigureAwait(false);
            }
            return;
        }

        // Full file transfer
        Context.SetResult(DavStatusCode.Ok);
        Context.Response.ContentLength = contentLength;

        // Disable buffering and flush headers
        responseBodyFeature?.DisableBuffering();
        await Context.Response.StartAsync(cancellationToken).ConfigureAwait(false);

        if (responseBodyFeature != null)
        {
            await responseBodyFeature.SendFileAsync(physicalPath, 0, contentLength > 0 ? contentLength : null, cancellationToken).ConfigureAwait(false);
        }
        else
        {
            // Fallback to stream
            await using var stream = new FileStream(physicalPath, FileMode.Open, FileAccess.Read, FileShare.Read,
                BufferPool.LargeBufferSize, FileOptions.Asynchronous | FileOptions.SequentialScan);
            await stream.CopyToPooledPipelinedAsync(Context.Response.Body, BufferPool.GetOptimalBufferSize(contentLength), cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Sends range data from a stream.
    /// </summary>
    private async Task SendRangeAsync(Stream stream, CancellationToken cancellationToken)
    {
        var requestHeaders = Context.Request.GetTypedHeaders();
        var range = requestHeaders.Range!.Ranges.First();
        var streamLength = stream.Length;

        if (!TryCalculateRange(range, streamLength, out var offset, out var length))
        {
            Context.SetResult(DavStatusCode.RequestedRangeNotSatisfiable);
            Context.Response.Headers["Content-Range"] = $"bytes */{streamLength}";
            return;
        }

        stream.Seek(offset, SeekOrigin.Begin);

        Context.SetResult(DavStatusCode.PartialContent);
        Context.Response.ContentLength = length;
        Context.Response.Headers["Content-Range"] = ResponseHeaderCache.GetContentRangeHeader(offset, offset + length - 1, streamLength);
        Context.Response.Headers["Accept-Ranges"] = ResponseHeaderCache.AcceptRangesBytes;

        // Disable buffering and flush headers
        var responseBodyFeature = Context.Features.Get<IHttpResponseBodyFeature>();
        responseBodyFeature?.DisableBuffering();
        await Context.Response.StartAsync(cancellationToken).ConfigureAwait(false);

        await stream.CopyToPooledAsync(Context.Response.Body, length, BufferPool.GetOptimalBufferSize(length), cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Sends full file content from a stream.
    /// </summary>
    private async Task SendFullContentAsync(Stream stream, long contentLength, CancellationToken cancellationToken)
    {
        Context.SetResult(DavStatusCode.Ok);

        if (stream.CanSeek)
        {
            Context.Response.Headers["Accept-Ranges"] = ResponseHeaderCache.AcceptRangesBytes;
            Context.Response.ContentLength = stream.Length;
        }
        else
        {
            Context.Response.Headers["Accept-Ranges"] = ResponseHeaderCache.AcceptRangesNone;
        }

        // Disable buffering and flush headers
        var responseBodyFeature = Context.Features.Get<IHttpResponseBodyFeature>();
        responseBodyFeature?.DisableBuffering();
        await Context.Response.StartAsync(cancellationToken).ConfigureAwait(false);

        var bufferSize = BufferPool.GetOptimalBufferSize(contentLength);
        if (contentLength > BufferPool.StreamingThreshold)
        {
            await stream.CopyToPooledPipelinedAsync(Context.Response.Body, bufferSize, cancellationToken).ConfigureAwait(false);
        }
        else
        {
            await stream.CopyToPooledAsync(Context.Response.Body, bufferSize, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Calculates byte range from a range header.
    /// </summary>
    private static bool TryCalculateRange(
        Microsoft.Net.Http.Headers.RangeItemHeaderValue range,
        long fileLength,
        out long offset,
        out long length)
    {
        offset = 0;
        length = fileLength;

        // Suffix range: last N bytes (e.g., "bytes=-500")
        if (range.From == null && range.To != null)
        {
            var suffixLength = Math.Min(range.To.Value, fileLength);
            offset = fileLength - suffixLength;
            length = suffixLength;
            return true;
        }

        // Open-ended range: from byte N to end (e.g., "bytes=500-")
        if (range.From != null && range.To == null)
        {
            if (range.From.Value >= fileLength)
                return false;

            offset = range.From.Value;
            length = fileLength - offset;
            return true;
        }

        // Bounded range (e.g., "bytes=500-999")
        if (range.From != null && range.To != null)
        {
            if (range.From.Value > range.To.Value || range.From.Value >= fileLength)
                return false;

            offset = range.From.Value;
            var effectiveTo = Math.Min(range.To.Value, fileLength - 1);
            length = effectiveTo - offset + 1;
            return true;
        }

        return true;
    }

    /// <summary>
    /// Gets a property value.
    /// </summary>
    private async Task<string?> GetPropertyValueAsync(XName propertyName, CancellationToken cancellationToken)
    {
        var metadata = PropertyManager.GetPropertyMetadata(Item!, propertyName);
        if (metadata?.Expensive == true)
            return null;

        var result = await PropertyManager.GetPropertyAsync(Item!, propertyName, cancellationToken);
        return result.Value?.ToString();
    }

    /// <summary>
    /// Gets the file name from the item URI.
    /// </summary>
    private string? GetFileName()
    {
        var path = Item!.Uri.AbsolutePath;
        var lastSlash = path.LastIndexOf('/');
        if (lastSlash >= 0 && lastSlash < path.Length - 1)
            return Uri.UnescapeDataString(path[(lastSlash + 1)..]);
        return null;
    }
}
