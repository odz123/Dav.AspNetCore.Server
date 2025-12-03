using Microsoft.Extensions.DependencyInjection;

namespace Dav.AspNetCore.Server;

public class WebDavOptionsBuilder : WebDavOptions
{
    /// <summary>
    /// Initializes a new <see cref="WebDavOptionsBuilder"/> class.
    /// </summary>
    /// <param name="services">The service collection.</param>
    internal WebDavOptionsBuilder(IServiceCollection services)
    {
        Services = services;
    }

    /// <summary>
    /// Gets the service collection.
    /// </summary>
    public IServiceCollection Services { get; }

    /// <summary>
    /// Configures the server for high-performance streaming workloads.
    /// Enables fast ETags and optimized file handling.
    /// </summary>
    /// <returns>The options builder for chaining.</returns>
    public WebDavOptionsBuilder UseHighPerformance()
    {
        UseFastETag = true;
        FastETagThreshold = 0; // Always use fast ETags
        return this;
    }

    /// <summary>
    /// Configures the server to always compute content-based ETags.
    /// This is slower but provides stronger cache validation.
    /// </summary>
    /// <returns>The options builder for chaining.</returns>
    public WebDavOptionsBuilder UseContentBasedETags()
    {
        UseFastETag = false;
        return this;
    }
}
