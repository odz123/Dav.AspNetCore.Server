using System.Globalization;
using System.Xml;
using Dav.AspNetCore.Server.Performance;
using Dav.AspNetCore.Server.Store.Properties;

namespace Dav.AspNetCore.Server.Store.Files;

public class File : IStoreItem, IPhysicalFileInfo, IOptimizedStreamable
{
    private readonly FileStore store;
    private readonly FileProperties properties;
    private string? _physicalPathCached;
    private bool _physicalPathResolved;

    /// <summary>
    /// Initializes a new <see cref="File"/> class.
    /// </summary>
    /// <param name="store">The file store</param>
    /// <param name="properties">The file properties.</param>
    public File(
        FileStore store,
        FileProperties properties)
    {
        ArgumentNullException.ThrowIfNull(store, nameof(store));
        ArgumentNullException.ThrowIfNull(properties, nameof(properties));

        this.store = store;
        this.properties = properties;
    }

    /// <summary>
    /// Gets the uri.
    /// </summary>
    public Uri Uri => properties.Uri;

    /// <summary>
    /// Gets the physical file path, if available.
    /// Used for zero-copy file transfers (SendFile optimization).
    /// </summary>
    public string PhysicalPath
    {
        get
        {
            if (!_physicalPathResolved)
            {
                _physicalPathCached = store.GetPhysicalPath(properties.Uri);
                _physicalPathResolved = true;
            }
            return _physicalPathCached ?? string.Empty;
        }
    }

    /// <summary>
    /// Gets a value indicating whether this file has a physical path for SendFile optimization.
    /// </summary>
    public bool HasPhysicalPath => !string.IsNullOrEmpty(PhysicalPath);

    /// <summary>
    /// Gets the file length without opening a stream.
    /// </summary>
    public long Length => properties.Length;

    /// <summary>
    /// Gets the last modification time without opening a stream.
    /// </summary>
    public DateTime LastModified => properties.LastModified;

    /// <summary>
    /// Gets a readable stream async.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The readable stream.</returns>
    public async Task<Stream> GetReadableStreamAsync(CancellationToken cancellationToken = default)
        => await store.OpenFileStreamAsync(properties.Uri, OpenFileMode.Read, cancellationToken);

    /// <summary>
    /// Gets a readable stream optimized for the specified access pattern.
    /// Uses OS-level hints like SequentialScan for streaming and RandomAccess for seeking.
    /// </summary>
    /// <param name="accessPattern">The expected access pattern.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>An optimized readable stream.</returns>
    public async Task<Stream> GetOptimizedReadableStreamAsync(
        Performance.FileAccessPattern accessPattern,
        CancellationToken cancellationToken = default)
        => await store.OpenOptimizedReadStreamAsync(properties.Uri, accessPattern, cancellationToken);

    /// <summary>
    /// Sets the data from the given stream async.
    /// </summary>
    /// <param name="stream">The stream.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The status code.</returns>
    public async Task<DavStatusCode> WriteDataAsync(Stream stream, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(stream, nameof(stream));

        await using var fileStream = await store.OpenFileStreamAsync(properties.Uri, OpenFileMode.Write, cancellationToken);

        // Use pooled buffer for copying
        await stream.CopyToPooledAsync(fileStream, BufferPool.LargeBufferSize, cancellationToken).ConfigureAwait(false);

        // Invalidate ETag cache since file was modified
        ETagCache.Instance.Invalidate(properties.Uri);

        return DavStatusCode.Ok;
    }

    /// <summary>
    /// Copies the store item to the destination store collection.
    /// </summary>
    /// <param name="destination">The destination.</param>
    /// <param name="name">The name.</param>
    /// <param name="overwrite">A value indicating whether the resource at the destination will be overwritten.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The operation result.</returns>
    public async Task<ItemResult> CopyAsync(
        IStoreCollection destination,
        string name,
        bool overwrite, 
        CancellationToken cancellationToken = default)
    {
        var item = await destination.GetItemAsync(name, cancellationToken);
        if (item != null && !overwrite)
            return ItemResult.Fail(DavStatusCode.PreconditionFailed);

        DavStatusCode statusCode;
        if (item != null && overwrite)
        {
            statusCode = await destination.DeleteItemAsync(name, cancellationToken);
            if (statusCode != DavStatusCode.NoContent)
                return ItemResult.Fail(DavStatusCode.PreconditionFailed);
        }

        var result = await destination.CreateItemAsync(name, cancellationToken).ConfigureAwait(false);
        if (result.Item == null)
            return ItemResult.Fail(result.StatusCode);

        await using var readableStream = await GetReadableStreamAsync(cancellationToken).ConfigureAwait(false);
        statusCode = await result.Item.WriteDataAsync(readableStream, cancellationToken).ConfigureAwait(false);
        if (statusCode != DavStatusCode.Ok)
            return ItemResult.Fail(statusCode);

        if (!store.DisableCaching)
            store.ItemCache.Set(result.Item.Uri.AbsolutePath, result.Item);

        return item != null
            ? ItemResult.NoContent(result.Item)
            : ItemResult.Created(result.Item);
    }
    
    private static string GetMimeTypeForFileExtension(Uri uri)
    {
        // Use cached MIME type lookup for better performance
        return MimeTypeCache.GetMimeType(uri);
    }

    private static async Task<string> ComputeEtagAsync(
        File file,
        CancellationToken cancellationToken = default)
    {
        // Use cached ETag computation - only recomputes if file has changed
        return await ETagCache.Instance.GetOrComputeAsync(
            file.properties.Uri,
            file.properties.Length,
            file.properties.LastModified,
            async () => await file.store.OpenFileStreamAsync(file.properties.Uri, OpenFileMode.Read, cancellationToken),
            cancellationToken).ConfigureAwait(false);
    }
    
    internal static void RegisterProperties()
    {
        Property.RegisterProperty<File>(
            XmlNames.CreationDate,
            read: (context, _) =>
            {
                context.SetResult(XmlConvert.ToString(((File)context.Item).properties.Created, XmlDateTimeSerializationMode.Utc)); 
                return ValueTask.CompletedTask;
            },
            metadata: new PropertyMetadata(Computed: true));
        
        Property.RegisterProperty<File>(
            XmlNames.DisplayName,
            read: (context, _) =>
            {
                context.SetResult(((File)context.Item).properties.Name); 
                return ValueTask.CompletedTask;
            },
            metadata: new PropertyMetadata(Computed: true));
        
        Property.RegisterProperty<File>(
            XmlNames.GetLastModified,
            read: (context, _) =>
            {
                context.SetResult(((File)context.Item).properties.LastModified.ToString("R")); 
                return ValueTask.CompletedTask;
            },
            metadata: new PropertyMetadata(Computed: true));
        
        Property.RegisterProperty<File>(
            XmlNames.GetContentLength,
            read: (context, _) =>
            {
                context.SetResult(((File)context.Item).properties.Length.ToString()); 
                return ValueTask.CompletedTask;
            },
            metadata: new PropertyMetadata(Computed: true));
        
        Property.RegisterProperty<File>(
            XmlNames.GetContentType,
            read: (context, _) =>
            {
                context.SetResult(GetMimeTypeForFileExtension(context.Item.Uri)); 
                return ValueTask.CompletedTask;
            },
            metadata: new PropertyMetadata(Computed: true));
        
        Property.RegisterProperty<File>(
            XmlNames.GetContentLanguage,
            read: (context, _) =>
            {
                context.SetResult(CultureInfo.CurrentCulture.TwoLetterISOLanguageName); 
                return ValueTask.CompletedTask;
            },
            metadata: new PropertyMetadata(Computed: true));
        
        Property.RegisterProperty<File>(
            XmlNames.GetEtag,
            read: async (context, cancellationToken) =>
            {
                var fileItem = (File)context.Item;
                var etag = await ComputeEtagAsync(fileItem, cancellationToken);

                context.SetResult(etag);
            },
            // No longer expensive due to caching - ETag is computed from metadata
            // and only reads file on first access or after modification
            metadata: new PropertyMetadata(Expensive: false, Computed: true));
        
        Property.RegisterProperty<File>(
            XmlNames.ResourceType,
            read: (context, _) =>
            {
                context.SetResult(null); 
                return ValueTask.CompletedTask;
            },
            metadata: new PropertyMetadata(Computed: true));

        Property.RegisterSupportedLockProperty<File>();
        Property.RegisterLockDiscoveryProperty<File>();
    }
}