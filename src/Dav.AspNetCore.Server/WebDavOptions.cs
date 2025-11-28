using Dav.AspNetCore.Server.Performance;

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
    /// Gets or sets the streaming options for optimized file transfers.
    /// Use StreamingOptions.ForNzbStreaming() for NZB/Usenet workloads.
    /// </summary>
    public StreamingOptions? Streaming { get; set; }

    /// <summary>
    /// Applies streaming configuration if set.
    /// Called automatically during startup.
    /// </summary>
    internal void ApplyStreamingOptions()
    {
        Streaming?.Apply();
    }
}