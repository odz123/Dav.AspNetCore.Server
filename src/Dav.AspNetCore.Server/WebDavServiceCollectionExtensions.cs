using Dav.AspNetCore.Server.Locks;
using Dav.AspNetCore.Server.Performance;
using Dav.AspNetCore.Server.Store.Properties;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Dav.AspNetCore.Server;

public static class WebDavServiceCollectionExtensions
{
    /// <summary>
    /// Adds the web dav services.
    /// </summary>
    /// <param name="services">The services.</param>
    /// <param name="webDavBuilder">The configure action.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddWebDav(
        this IServiceCollection services,
        Action<WebDavOptionsBuilder> webDavBuilder)
    {
        ArgumentNullException.ThrowIfNull(services, nameof(services));
        ArgumentNullException.ThrowIfNull(webDavBuilder, nameof(webDavBuilder));

        var builder = new WebDavOptionsBuilder(services);
        webDavBuilder(builder);

        // Apply ETag configuration
        ETagCache.AlwaysUseFastETag = builder.UseFastETag && builder.FastETagThreshold == 0;
        ETagCache.FastETagThreshold = builder.FastETagThreshold;

        services.AddHttpContextAccessor();

        services.TryAddSingleton<WebDavOptions>(builder);
        services.TryAddScoped<IPropertyManager, PropertyManager>();
        services.TryAddSingleton<ILockManager>(new InMemoryLockManager(Array.Empty<ResourceLock>()));

        return services;
    }
}
