using Dav.AspNetCore.Server.Performance;
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
    /// Configures the server for NZB/Usenet streaming workloads.
    /// Enables fast ETags, optimized buffering, and zero-copy transfers.
    /// </summary>
    /// <returns>The options builder for chaining.</returns>
    public WebDavOptionsBuilder UseNzbStreaming()
    {
        Streaming = StreamingOptions.ForNzbStreaming();
        return this;
    }

    /// <summary>
    /// Configures the server for video streaming workloads.
    /// Enables fast ETags and optimized range request handling.
    /// </summary>
    /// <returns>The options builder for chaining.</returns>
    public WebDavOptionsBuilder UseVideoStreaming()
    {
        Streaming = StreamingOptions.ForVideoStreaming();
        return this;
    }

    /// <summary>
    /// Configures the server for low-latency access.
    /// Prioritizes time-to-first-byte over throughput.
    /// </summary>
    /// <returns>The options builder for chaining.</returns>
    public WebDavOptionsBuilder UseLowLatency()
    {
        Streaming = StreamingOptions.ForLowLatency();
        return this;
    }

    /// <summary>
    /// Configures custom streaming options.
    /// </summary>
    /// <param name="configure">Action to configure streaming options.</param>
    /// <returns>The options builder for chaining.</returns>
    public WebDavOptionsBuilder UseStreaming(Action<StreamingOptions> configure)
    {
        Streaming = new StreamingOptions();
        configure(Streaming);
        return this;
    }
}