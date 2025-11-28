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
    /// Optimized for fast stream starts and efficient seeking.
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

        var requestHeaders = Context.Request.GetTypedHeaders();

        // OPTIMIZATION: Determine access pattern early (before opening stream)
        // This allows us to use appropriate FileOptions and avoid unnecessary stream opens
        var hasRangeRequest = requestHeaders.Range != null &&
                             requestHeaders.Range.Unit.Equals("bytes") &&
                             requestHeaders.Range.Ranges.Count == 1;

        // Check for optimized streaming interface
        var optimizedItem = Item as IOptimizedStreamable;
        var physicalFile = Item as IPhysicalFileInfo;
        var physicalPath = physicalFile?.PhysicalPath;
        var hasPhysicalPath = !string.IsNullOrEmpty(physicalPath);

        // OPTIMIZATION: For HEAD requests with IOptimizedStreamable, avoid opening stream entirely
        var isHeadRequest = Context.Request.Method == WebDavMethods.Head;

        // Get content length early - without opening stream if possible
        long contentLength;
        if (optimizedItem != null)
        {
            // Fast path: get length from cached metadata
            contentLength = optimizedItem.Length;
        }
        else
        {
            // Fallback: parse from properties
            var contentLengthStr = await GetNonExpensivePropertyAsync(Item, XmlNames.GetContentLength, cancellationToken);
            contentLength = long.TryParse(contentLengthStr, out var len) ? len : -1L;
        }

        // Fetch essential properties in parallel
        var properties = await GetPropertiesParallelAsync(Item, cancellationToken);

        // Set response headers early to reduce TTFB
        SetResponseHeaders(properties, contentLength);

        // Determine if ranges should be disabled based on IfRange
        var disableRanges = CheckIfRangeCondition(requestHeaders, properties);

        // OPTIMIZATION: Handle HEAD requests without opening the stream
        if (isHeadRequest)
        {
            await HandleHeadRequestAsync(contentLength, hasPhysicalPath || optimizedItem != null);
            return;
        }

        // Determine the actual access pattern for stream optimization
        var accessPattern = (hasRangeRequest && !disableRanges)
            ? FileAccessPattern.RandomAccess
            : FileAccessPattern.Sequential;

        // OPTIMIZATION: For physical files with SendFile support, use zero-copy transfer
        if (hasPhysicalPath && !disableRanges)
        {
            await SendFileZeroCopyAsync(physicalPath!, contentLength, hasRangeRequest, cancellationToken);
            return;
        }

        // Open stream with optimized access pattern
        Stream readableStream;
        if (optimizedItem != null)
        {
            // Use optimized stream with appropriate FileOptions
            readableStream = await optimizedItem.GetOptimizedReadableStreamAsync(accessPattern, cancellationToken);
        }
        else
        {
            // Fallback to regular stream
            readableStream = await Item.GetReadableStreamAsync(cancellationToken);
        }

        await using (readableStream)
        {
            if (hasRangeRequest && !disableRanges && readableStream.CanSeek)
            {
                await SendRangeDataAsync(Context, readableStream, cancellationToken);
            }
            else
            {
                await SendFullDataAsync(Context, readableStream, contentLength, cancellationToken);
            }
        }
    }

    /// <summary>
    /// Handles HEAD requests without opening the file stream.
    /// This is a significant optimization for clients that probe file metadata.
    /// </summary>
    private Task HandleHeadRequestAsync(long contentLength, bool supportsRanges)
    {
        // Use pre-computed header values for faster response
        Context.Response.Headers["Accept-Ranges"] = supportsRanges
            ? ResponseHeaderCache.AcceptRangesBytes
            : ResponseHeaderCache.AcceptRangesNone;

        if (contentLength >= 0)
        {
            Context.Response.ContentLength = contentLength;
        }

        AddStreamingHeaders(Context, Context.Response.ContentType, contentLength);
        Context.SetResult(DavStatusCode.Ok);
        return Task.CompletedTask;
    }

    /// <summary>
    /// Sets response headers from pre-fetched properties.
    /// Called early to reduce time-to-first-byte.
    /// </summary>
    private void SetResponseHeaders(FilePropertySet properties, long contentLength)
    {
        if (!string.IsNullOrWhiteSpace(properties.ContentType))
            Context.Response.Headers["Content-Type"] = properties.ContentType;

        if (!string.IsNullOrWhiteSpace(properties.ContentLanguage))
            Context.Response.Headers["Content-Language"] = properties.ContentLanguage;

        if (!string.IsNullOrWhiteSpace(properties.LastModified))
            Context.Response.Headers["Last-Modified"] = properties.LastModified;

        if (!string.IsNullOrWhiteSpace(properties.ETag))
            Context.Response.Headers["ETag"] = $"\"{properties.ETag}\"";

        if (contentLength >= 0)
            Context.Response.Headers["Content-Length"] = contentLength.ToString();

        // Add Content-Disposition header for file downloads
        var fileName = GetFileName(Item!.Uri);
        if (!string.IsNullOrWhiteSpace(fileName))
        {
            var encodedFileName = WebUtility.UrlEncode(fileName);
            Context.Response.Headers["Content-Disposition"] = $"inline; filename=\"{fileName}\"; filename*=UTF-8''{encodedFileName}";
        }
    }

    /// <summary>
    /// Checks If-Range precondition and returns whether ranges should be disabled.
    /// </summary>
    private static bool CheckIfRangeCondition(
        Microsoft.AspNetCore.Http.Headers.RequestHeaders requestHeaders,
        FilePropertySet properties)
    {
        if (requestHeaders.IfRange == null)
            return false;

        // Check ETag mismatch
        if (requestHeaders.IfRange.EntityTag != null &&
            !string.IsNullOrWhiteSpace(properties.ETag) &&
            requestHeaders.IfRange.EntityTag.Tag != $"\"{properties.ETag}\"")
        {
            return true;
        }

        // Check Last-Modified mismatch
        if (requestHeaders.IfRange.LastModified != null &&
            !string.IsNullOrWhiteSpace(properties.LastModified) &&
            DateTimeOffset.TryParse(properties.LastModified, out var parsedLastModified) &&
            requestHeaders.IfRange.LastModified != parsedLastModified)
        {
            return true;
        }

        return false;
    }

    /// <summary>
    /// Sends file using zero-copy SendFileAsync - the fastest possible method.
    /// Handles both full file and range requests using kernel-level optimization.
    /// </summary>
    private async Task SendFileZeroCopyAsync(
        string physicalPath,
        long contentLength,
        bool hasRangeRequest,
        CancellationToken cancellationToken)
    {
        var requestHeaders = Context.Request.GetTypedHeaders();
        var responseBodyFeature = Context.Features.Get<IHttpResponseBodyFeature>();

        Context.Response.Headers["Accept-Ranges"] = ResponseHeaderCache.AcceptRangesBytes;
        AddStreamingHeaders(Context, Context.Response.ContentType, contentLength);

        // Handle range requests using SendFile with offset/length
        if (hasRangeRequest)
        {
            var range = requestHeaders.Range!.Ranges.First();

            // Calculate range parameters using known content length
            if (!TryCalculateRange(range, contentLength, out var offset, out var length))
            {
                Context.SetResult(DavStatusCode.RequestedRangeNotSatisfiable);
                Context.Response.Headers["Content-Range"] = $"bytes */{contentLength}";
                return;
            }

            // OPTIMIZATION: Track seek pattern and get prefetch hints
            var filePath = Item?.Uri.AbsolutePath ?? string.Empty;
            var (pattern, prefetchHint) = SeekPatternTracker.Instance.RecordAccessWithPrefetch(
                filePath, offset, length, contentLength);

            // Queue background prefetch for predicted next request
            if (prefetchHint.HasValue && prefetchHint.Value.Confidence >= 3)
            {
                PrefetchService.Instance.QueuePrefetch(physicalPath, prefetchHint.Value);
            }

            Context.SetResult(DavStatusCode.PartialContent);
            Context.Response.ContentLength = length;
            Context.Response.Headers["Content-Range"] = ResponseHeaderCache.GetContentRangeHeader(offset, offset + length - 1, contentLength);

            // OPTIMIZATION: Flush headers immediately to reduce seek response latency
            await Context.Response.StartAsync(cancellationToken).ConfigureAwait(false);

            // OPTIMIZATION: Try memory-mapped file for random access patterns (fastest seeking)
            if (pattern == FileAccessPattern.RandomAccess &&
                MemoryMappedFilePool.ShouldUseMemoryMapping(contentLength, pattern))
            {
                var lastModified = System.IO.File.GetLastWriteTimeUtc(physicalPath);
                var mappedStream = MemoryMappedFilePool.Instance.GetMappedRangeStream(
                    physicalPath, contentLength, lastModified, offset, length);

                if (mappedStream != null)
                {
                    await using (mappedStream)
                    {
                        var bufferSize = OptimizedFileStream.GetOptimalBufferSize(length, pattern);
                        await mappedStream.CopyToPooledAsync(Context.Response.Body, length, bufferSize, cancellationToken).ConfigureAwait(false);
                    }
                    return;
                }
            }

            if (responseBodyFeature != null)
            {
                // Zero-copy range transfer - kernel handles seeking
                await responseBodyFeature.SendFileAsync(physicalPath, offset, length, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                // Fallback: open stream with pattern-based hint
                await using var stream = OptimizedFileStream.OpenForRead(physicalPath, pattern);
                stream.Seek(offset, SeekOrigin.Begin);
                var bufferSize = OptimizedFileStream.GetOptimalBufferSize(length, pattern);
                await stream.CopyToPooledAsync(Context.Response.Body, length, bufferSize, cancellationToken).ConfigureAwait(false);
            }
            return;
        }

        // Full file transfer using SendFileAsync (zero-copy)
        Context.SetResult(DavStatusCode.Ok);
        Context.Response.ContentLength = contentLength;

        if (responseBodyFeature != null)
        {
            await responseBodyFeature.SendFileAsync(physicalPath, 0, contentLength > 0 ? contentLength : null, cancellationToken).ConfigureAwait(false);
        }
        else
        {
            // Fallback: open stream with Sequential hint for read-ahead
            await using var stream = OptimizedFileStream.OpenForSequentialRead(physicalPath);
            var bufferSize = OptimizedFileStream.GetOptimalBufferSize(contentLength, FileAccessPattern.Sequential);
            await stream.CopyToPooledAsync(Context.Response.Body, bufferSize, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Tries to calculate the byte range from a range header.
    /// Returns false if the range is unsatisfiable.
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
    /// Sends range data from a stream (for non-physical files).
    /// </summary>
    private async Task SendRangeDataAsync(
        HttpContext context,
        Stream stream,
        CancellationToken cancellationToken)
    {
        var requestHeaders = context.Request.GetTypedHeaders();
        var range = requestHeaders.Range!.Ranges.First();
        var streamLength = stream.Length;

        if (!TryCalculateRange(range, streamLength, out var offset, out var length))
        {
            context.SetResult(DavStatusCode.RequestedRangeNotSatisfiable);
            context.Response.Headers["Content-Range"] = $"bytes */{streamLength}";
            return;
        }

        // OPTIMIZATION: Track seek pattern and get prefetch hints
        var filePath = Item?.Uri.AbsolutePath ?? string.Empty;
        var (pattern, prefetchHint) = SeekPatternTracker.Instance.RecordAccessWithPrefetch(
            filePath, offset, length, streamLength);

        // Queue background prefetch for predicted next request (if physical path available)
        var physicalFile = Item as IPhysicalFileInfo;
        if (prefetchHint.HasValue && prefetchHint.Value.Confidence >= 3 &&
            !string.IsNullOrEmpty(physicalFile?.PhysicalPath))
        {
            PrefetchService.Instance.QueuePrefetch(physicalFile.PhysicalPath, prefetchHint.Value);
        }

        // Seek to the start position
        stream.Seek(offset, SeekOrigin.Begin);

        context.SetResult(DavStatusCode.PartialContent);
        context.Response.ContentLength = length;
        context.Response.Headers["Content-Range"] = ResponseHeaderCache.GetContentRangeHeader(offset, offset + length - 1, streamLength);
        context.Response.Headers["Accept-Ranges"] = ResponseHeaderCache.AcceptRangesBytes;

        // OPTIMIZATION: Flush headers immediately for fast seek response
        await context.Response.StartAsync(cancellationToken).ConfigureAwait(false);

        // Use buffer size based on detected access pattern
        var bufferSize = OptimizedFileStream.GetOptimalBufferSize(length, pattern);
        await stream.CopyToPooledAsync(context.Response.Body, length, bufferSize, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Sends full file data from a stream.
    /// </summary>
    private static async Task SendFullDataAsync(
        HttpContext context,
        Stream stream,
        long contentLength,
        CancellationToken cancellationToken)
    {
        context.SetResult(DavStatusCode.Ok);

        if (stream.CanSeek)
        {
            context.Response.Headers["Accept-Ranges"] = ResponseHeaderCache.AcceptRangesBytes;
            context.Response.ContentLength = stream.Length;
        }
        else
        {
            context.Response.Headers["Accept-Ranges"] = ResponseHeaderCache.AcceptRangesNone;
        }

        AddStreamingHeaders(context, context.Response.ContentType, contentLength);

        // OPTIMIZATION: Flush headers immediately to reduce TTFB
        // This allows the client to start processing headers while we read the file
        await context.Response.StartAsync(cancellationToken).ConfigureAwait(false);

        // Use optimal buffer size for sequential reads
        var bufferSize = OptimizedFileStream.GetOptimalBufferSize(contentLength, FileAccessPattern.Sequential);

        // OPTIMIZATION: Use pipelined copy for large files to overlap read and write operations
        if (contentLength > BufferPool.StreamingThreshold)
        {
            await stream.CopyToPooledPipelinedAsync(context.Response.Body, bufferSize, cancellationToken).ConfigureAwait(false);
        }
        else
        {
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
    /// Adds streaming-optimized headers to the response using pre-computed values.
    /// </summary>
    private static void AddStreamingHeaders(HttpContext context, string? contentType, long contentLength)
    {
        // Allow clients to cache streamable content
        if (IsStreamableContent(contentType))
        {
            // Use pre-computed header value
            context.Response.Headers["Cache-Control"] = ResponseHeaderCache.CacheControlStreaming;
        }

        // Disable response buffering for streaming - critical for fast TTFB
        var bufferingFeature = context.Features.Get<IHttpResponseBodyFeature>();
        bufferingFeature?.DisableBuffering();

        // For large files, hint that the connection should be kept alive
        if (contentLength > BufferPool.StreamingThreshold)
        {
            context.Response.Headers["Connection"] = ResponseHeaderCache.ConnectionKeepAlive;
            context.Response.Headers["Keep-Alive"] = ResponseHeaderCache.GetKeepAliveHeader(120);
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
}
