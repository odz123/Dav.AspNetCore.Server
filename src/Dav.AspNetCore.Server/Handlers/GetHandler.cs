using System.Xml.Linq;
using Dav.AspNetCore.Server.Http;
using Dav.AspNetCore.Server.Store;
using Microsoft.AspNetCore.Http;

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

        var contentType = await GetNonExpensivePropertyAsync(Item, XmlNames.GetContentType, cancellationToken);
        if (!string.IsNullOrWhiteSpace(contentType))
            Context.Response.Headers["Content-Type"] = contentType;

        var contentLanguage = await GetNonExpensivePropertyAsync(Item, XmlNames.GetContentLanguage, cancellationToken);
        if (!string.IsNullOrWhiteSpace(contentLanguage))
            Context.Response.Headers["Content-Language"] = contentLanguage;

        var lastModified = await GetNonExpensivePropertyAsync(Item, XmlNames.GetLastModified, cancellationToken);
        if (!string.IsNullOrWhiteSpace(lastModified))
            Context.Response.Headers["Last-Modified"] = lastModified;

        var etag = await GetNonExpensivePropertyAsync(Item, XmlNames.GetEtag, cancellationToken);
        if (!string.IsNullOrWhiteSpace(etag))
            Context.Response.Headers["ETag"] = etag;
        
        var contentLength = await GetNonExpensivePropertyAsync(Item, XmlNames.GetContentLength, cancellationToken);
        if (!string.IsNullOrWhiteSpace(contentLength))
            Context.Response.Headers["Content-Length"] = contentLength;

        await using var readableStream = await Item.GetReadableStreamAsync(cancellationToken);
        if (readableStream.CanSeek)
        {
            Context.Response.Headers["Accept-Ranges"] = "bytes";
            Context.Response.ContentLength = readableStream.Length;
        }
        else
        {
            Context.Response.Headers["Accept-Ranges"] = "none";
        }

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
                !string.IsNullOrWhiteSpace(etag) &&
                requestHeaders.IfRange.EntityTag.Tag != $"\"{etag}\"")
            {
                disableRanges = true;
            }

            if (requestHeaders.IfRange.LastModified != null &&
                !string.IsNullOrWhiteSpace(lastModified) &&
                DateTimeOffset.TryParse(lastModified, out var parsedLastModified) &&
                requestHeaders.IfRange.LastModified != parsedLastModified)
            {
                disableRanges = true;
            }
        }

        await SendDataAsync(Context, readableStream, disableRanges, cancellationToken);
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
    
    private static async Task SendDataAsync(
        HttpContext context,
        Stream stream,
        bool disableRanges,
        CancellationToken cancellationToken = default)
    {
        var requestHeaders = context.Request.GetTypedHeaders();

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

            while (bytesToRead > 0)
            {
                var buffer = new byte[Math.Min(bytesToRead, 1024 * 64)];
                var bytesRead = await stream.ReadAsync(buffer, 0, buffer.Length, cancellationToken);
                if (bytesRead == 0)
                    break; // End of stream reached unexpectedly

                await context.Response.Body.WriteAsync(buffer, 0, bytesRead, cancellationToken);

                bytesToRead -= bytesRead;
            }

            return;
        }
        
        context.SetResult(DavStatusCode.Ok);
        await stream.CopyToAsync(context.Response.Body, cancellationToken);
    }
}