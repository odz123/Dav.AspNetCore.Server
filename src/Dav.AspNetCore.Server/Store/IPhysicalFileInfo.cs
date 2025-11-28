namespace Dav.AspNetCore.Server.Store;

/// <summary>
/// Interface for store items that have a physical file path.
/// When implemented, enables zero-copy file transfers using OS-level optimizations.
/// </summary>
public interface IPhysicalFileInfo
{
    /// <summary>
    /// Gets the physical file path on the local file system.
    /// </summary>
    string PhysicalPath { get; }
}
