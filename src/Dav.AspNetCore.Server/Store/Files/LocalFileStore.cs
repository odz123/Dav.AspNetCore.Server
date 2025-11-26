namespace Dav.AspNetCore.Server.Store.Files;

public class LocalFileStore : FileStore
{
    private readonly LocalFileStoreOptions options;
    private readonly string normalizedRootPath;

    /// <summary>
    /// Initializes a new <see cref="LocalFileStore"/> class.
    /// </summary>
    /// <param name="options">The local file store options.</param>
    public LocalFileStore(LocalFileStoreOptions options)
    {
        ArgumentNullException.ThrowIfNull(options, nameof(options));
        this.options = options;
        // Normalize the root path once at construction and ensure it ends with separator
        normalizedRootPath = Path.GetFullPath(options.RootPath).TrimEnd(Path.DirectorySeparatorChar) + Path.DirectorySeparatorChar;
    }

    /// <summary>
    /// Safely combines root path with URI path and validates against path traversal.
    /// </summary>
    /// <param name="uri">The URI to combine with root path.</param>
    /// <returns>The validated full path.</returns>
    /// <exception cref="InvalidOperationException">Thrown when path traversal is detected.</exception>
    private string GetSafePath(Uri uri)
    {
        var combinedPath = Path.Combine(options.RootPath, uri.LocalPath.TrimStart('/'));
        var fullPath = Path.GetFullPath(combinedPath);

        // Ensure the resolved path is within the root directory
        // We use normalizedRootPath which ends with separator to prevent partial matches
        // (e.g., /var/webdav-other should not match root /var/webdav)
        if (!fullPath.StartsWith(normalizedRootPath, StringComparison.OrdinalIgnoreCase) &&
            !fullPath.Equals(normalizedRootPath.TrimEnd(Path.DirectorySeparatorChar), StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException($"Access denied: Path traversal detected.");
        }

        return fullPath;
    }

    public override ValueTask<bool> DirectoryExistsAsync(Uri uri, CancellationToken cancellationToken = default)
    {
        var path = GetSafePath(uri);
        return ValueTask.FromResult(System.IO.Directory.Exists(path));
    }

    public override ValueTask<bool> FileExistsAsync(Uri uri, CancellationToken cancellationToken = default)
    {
        var path = GetSafePath(uri);
        return ValueTask.FromResult(System.IO.File.Exists(path));
    }

    public override ValueTask DeleteDirectoryAsync(Uri uri, CancellationToken cancellationToken = default)
    {
        var path = GetSafePath(uri);
        System.IO.Directory.Delete(path, recursive: true);

        return ValueTask.CompletedTask;
    }

    public override ValueTask DeleteFileAsync(Uri uri, CancellationToken cancellationToken = default)
    {
        var path = GetSafePath(uri);
        System.IO.File.Delete(path);

        return ValueTask.CompletedTask;
    }

    public override ValueTask<DirectoryProperties> GetDirectoryPropertiesAsync(Uri uri, CancellationToken cancellationToken = default)
    {
        var path = GetSafePath(uri);
        var directoryInfo = new DirectoryInfo(path);
        var directoryProperties = new DirectoryProperties(
            uri,
            directoryInfo.Name,
            directoryInfo.CreationTimeUtc,
            directoryInfo.LastWriteTimeUtc);

        return ValueTask.FromResult(directoryProperties);
    }

    public override ValueTask<FileProperties> GetFilePropertiesAsync(Uri uri, CancellationToken cancellationToken = default)
    {
        var path = GetSafePath(uri);
        var fileInfo = new FileInfo(path);
        var fileProperties = new FileProperties(
            uri,
            fileInfo.Name,
            fileInfo.CreationTimeUtc,
            fileInfo.LastWriteTimeUtc,
            fileInfo.Length);

        return ValueTask.FromResult(fileProperties);
    }

    public override ValueTask<Stream> OpenFileStreamAsync(Uri uri, OpenFileMode mode, CancellationToken cancellationToken = default)
    {
        var path = GetSafePath(uri);
        return ValueTask.FromResult<Stream>(mode == OpenFileMode.Read
            ? System.IO.File.OpenRead(path)
            : System.IO.File.OpenWrite(path));
    }

    public override ValueTask CreateDirectoryAsync(Uri uri, CancellationToken cancellationToken)
    {
        var path = GetSafePath(uri);
        System.IO.Directory.CreateDirectory(path);

        return ValueTask.CompletedTask;
    }

    public override ValueTask<Uri[]> GetFilesAsync(Uri uri, CancellationToken cancellationToken)
    {
        var path = GetSafePath(uri);
        return ValueTask.FromResult(System.IO.Directory.GetFiles(path).Select(x =>
        {
            var relativePath = $"/{Path.GetRelativePath(options.RootPath, x)}";
            return new Uri(relativePath);
        }).ToArray());
    }

    public override ValueTask<Uri[]> GetDirectoriesAsync(Uri uri, CancellationToken cancellationToken)
    {
        var path = GetSafePath(uri);
        return ValueTask.FromResult(System.IO.Directory.GetDirectories(path).Select(x =>
        {
            var relativePath = $"/{Path.GetRelativePath(options.RootPath, x)}";
            return new Uri(relativePath);
        }).ToArray());
    }
}