namespace Dav.AspNetCore.Server;

public class WebDavOptions
{
    /// <summary>
    /// Disallows propfind requests with depth set to infinity.
    /// </summary>
    public bool DisallowInfinityDepth { get; set; }

    /// <summary>
    /// Gets or sets the server name.
    /// </summary>
    public string? ServerName { get; set; }

    /// <summary>
    /// A value indicating whether the server header is disabled.
    /// </summary>
    public bool DisableServerName { get; set; }

    /// <summary>
    /// Gets or sets the maximum lock timeout.
    /// </summary>
    public TimeSpan? MaxLockTimeout { get; set; } = null;

    /// <summary>
    /// A value indicating whether web dav requires authentication.
    /// </summary>
    public bool RequiresAuthentication { get; set; }

    /// <summary>
    /// Gets or sets whether to use fast (metadata-based) ETags for large files.
    /// When enabled, ETags are computed from file size and modification time instead of content hash.
    /// Default is true for better performance with large files.
    /// </summary>
    public bool UseFastETag { get; set; } = true;

    /// <summary>
    /// Gets or sets the threshold in bytes above which fast ETags are used.
    /// Files larger than this will use metadata-based ETags.
    /// Default is 10MB. Set to 0 to always use fast ETags.
    /// </summary>
    public long FastETagThreshold { get; set; } = 10 * 1024 * 1024;
}
