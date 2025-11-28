using Dav.AspNetCore.Server.Performance;

namespace Dav.AspNetCore.Server.Store;

/// <summary>
/// Interface for store items that support optimized streaming with access pattern hints.
/// When implemented, enables OS-level optimizations like SequentialScan and RandomAccess.
/// </summary>
public interface IOptimizedStreamable
{
    /// <summary>
    /// Gets a readable stream optimized for the specified access pattern.
    /// </summary>
    /// <param name="accessPattern">The expected access pattern.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>An optimized readable stream.</returns>
    Task<Stream> GetOptimizedReadableStreamAsync(
        FileAccessPattern accessPattern,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets the file length without opening a stream.
    /// This enables fast HEAD request handling and range request validation.
    /// </summary>
    long Length { get; }

    /// <summary>
    /// Gets the last modification time without opening a stream.
    /// </summary>
    DateTime LastModified { get; }
}
