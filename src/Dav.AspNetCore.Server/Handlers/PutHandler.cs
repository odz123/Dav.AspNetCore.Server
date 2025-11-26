namespace Dav.AspNetCore.Server.Handlers;

internal class PutHandler : RequestHandler
{
    /// <summary>
    /// Handles the web dav request async.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns></returns>
    protected override async Task HandleRequestAsync(CancellationToken cancellationToken = default)
    {
        var requestUri = Context.Request.Path.ToUri();
        var itemName = requestUri.GetRelativeUri(Collection.Uri).LocalPath.Trim('/');
        var itemExisted = Item != null;
        var result = await Collection.CreateItemAsync(itemName, cancellationToken);
        if (result.Item == null)
        {
            Context.SetResult(result.StatusCode);
            return;
        }

        var writeResult = await result.Item.WriteDataAsync(Context.Request.Body, cancellationToken);
        if (writeResult != DavStatusCode.Ok)
        {
            Context.SetResult(writeResult);
            return;
        }

        Context.SetResult(itemExisted ? DavStatusCode.NoContent : DavStatusCode.Created);
    }
}