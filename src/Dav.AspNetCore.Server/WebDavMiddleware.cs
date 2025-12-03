using System.Diagnostics;
using Dav.AspNetCore.Server.Http;
using Dav.AspNetCore.Server.Store;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Dav.AspNetCore.Server;

internal sealed partial class WebDavMiddleware
{
    private readonly WebDavOptions webDavOptions;
    private readonly ILogger<WebDavMiddleware> logger;

    private static readonly string DefaultServerName = $"Dav.AspNetCore.Server/{typeof(WebDavMiddleware).Assembly.GetName().Version}";

    /// <summary>
    /// Initializes a new <see cref="WebDavMiddleware"/> class.
    /// </summary>
    /// <param name="next">The next request delegate.</param>
    /// <param name="webDavOptions">The web dav options.</param>
    /// <param name="logger">The logger.</param>
    public WebDavMiddleware(
        RequestDelegate next,
        WebDavOptions webDavOptions,
        ILogger<WebDavMiddleware> logger)
    {
        this.webDavOptions = webDavOptions;
        this.logger = logger;
    }

    /// <summary>
    /// Invokes the middleware async.
    /// </summary>
    /// <param name="context">The http context.</param>
    /// <exception cref="InvalidOperationException"></exception>
    public async Task InvokeAsync(HttpContext context)
    {
        var stopwatch = Stopwatch.StartNew();

        if (webDavOptions.RequiresAuthentication &&
            context.Request.Method != WebDavMethods.Options &&
            context.User.Identity?.IsAuthenticated != true)
        {
            await context.ChallengeAsync();
            return;
        }

        var resourceStore = context.RequestServices.GetService<IStore>();
        if (resourceStore == null)
            throw new InvalidOperationException("MapWebDav was used but it was never added. Use AddWebDav during service configuration.");

        if (!webDavOptions.DisableServerName)
            context.Response.Headers["Server"] = string.IsNullOrWhiteSpace(webDavOptions.ServerName)
                ? DefaultServerName
                : webDavOptions.ServerName;

        if (!RequestHandlerFactory.TryGetRequestHandler(context.Request.Method, out var handler))
        {
            LogMethodNotImplemented(logger, context.Request.Method);
            context.Response.StatusCode = StatusCodes.Status501NotImplemented;
            return;
        }

        LogRequestStarting(logger, context.Request.Method, context.Request.Path);

        try
        {
            await handler.HandleRequestAsync(context, resourceStore, context.RequestAborted).ConfigureAwait(false);
        }
        catch (Exception e)
        {
            stopwatch.Stop();
            LogRequestError(logger, e, context.Request.Method, context.Request.Path, stopwatch.ElapsedMilliseconds);

            if (!context.Response.HasStarted)
                context.Response.StatusCode = StatusCodes.Status500InternalServerError;
            return;
        }

        stopwatch.Stop();
        LogRequestFinished(logger, context.Request.Method, context.Request.Path, context.Response.StatusCode, stopwatch.ElapsedMilliseconds);
    }

    // Source-generated logging methods for zero-allocation logging
    [LoggerMessage(Level = LogLevel.Information, Message = "Request {Method} is not implemented")]
    private static partial void LogMethodNotImplemented(ILogger logger, string method);

    [LoggerMessage(Level = LogLevel.Information, Message = "Request starting {Method} {Path}")]
    private static partial void LogRequestStarting(ILogger logger, string method, PathString path);

    [LoggerMessage(Level = LogLevel.Error, Message = "Unexpected error while handling request {Method} {Path} in {ElapsedMs}ms")]
    private static partial void LogRequestError(ILogger logger, Exception exception, string method, PathString path, long elapsedMs);

    [LoggerMessage(Level = LogLevel.Information, Message = "Request finished {Method} {Path} {StatusCode} in {ElapsedMs}ms")]
    private static partial void LogRequestFinished(ILogger logger, string method, PathString path, int statusCode, long elapsedMs);
}
