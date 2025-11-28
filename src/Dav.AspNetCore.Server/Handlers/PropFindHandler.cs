using System.Collections.Concurrent;
using System.Xml.Linq;
using Dav.AspNetCore.Server.Http.Headers;
using Dav.AspNetCore.Server.Store;
using Dav.AspNetCore.Server.Store.Properties;
using Microsoft.AspNetCore.Http;

namespace Dav.AspNetCore.Server.Handlers;

internal class PropFindHandler : RequestHandler
{
    // Maximum degree of parallelism for property retrieval
    private const int MaxParallelism = 8;

    /// <summary>
    /// Handles the web dav request async.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns></returns>
    protected override async Task HandleRequestAsync(CancellationToken cancellationToken = default)
    {
        if (Item == null)
        {
            Context.SetResult(DavStatusCode.NotFound);
            return;
        }

        var items = new List<IStoreItem>();
        if (Item is IStoreCollection collection)
        {
            var headers = Context.Request.GetTypedWebDavHeaders();
            if (headers.Depth == Depth.Infinity && Options.DisallowInfinityDepth)
            {
                Context.SetResult(DavStatusCode.Forbidden);
                return;
            }

            var depth = headers.Depth ?? (Options.DisallowInfinityDepth ? Depth.One : Depth.Infinity);
            await AddItemsRecursive(collection, depth, 0, items, cancellationToken).ConfigureAwait(false);
        }
        else
        {
            items.Add(Item);
        }

        var requestedProperties = await GetRequestedPropertiesAsync(Context, cancellationToken).ConfigureAwait(false);

        var multiStatus = new XElement(XmlNames.MultiStatus);
        var document = new XDocument(
            new XDeclaration("1.0", "utf-8", null),
            multiStatus);

        var pathBase = Context.Request.PathBase.ToUriComponent();

        // For large collections, use parallel property retrieval
        if (items.Count > 10)
        {
            var propertyResults = new ConcurrentDictionary<IStoreItem, Dictionary<XName, PropertyResult>>();

            // Fetch properties in parallel with limited concurrency
            var semaphore = new SemaphoreSlim(MaxParallelism);
            var tasks = items.Select(async item =>
            {
                await semaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
                try
                {
                    var props = await GetPropertiesAsync(item, requestedProperties, cancellationToken).ConfigureAwait(false);
                    propertyResults[item] = props;
                }
                finally
                {
                    semaphore.Release();
                }
            });

            await Task.WhenAll(tasks).ConfigureAwait(false);

            // Build response in order
            foreach (var item in items)
            {
                var response = BuildResponseElement(item, propertyResults[item], pathBase);
                multiStatus.Add(response);
            }
        }
        else
        {
            // For small collections, use sequential processing to avoid overhead
            foreach (var item in items)
            {
                var propertyValues = await GetPropertiesAsync(item, requestedProperties, cancellationToken).ConfigureAwait(false);
                var response = BuildResponseElement(item, propertyValues, pathBase);
                multiStatus.Add(response);
            }
        }

        await Context.WriteDocumentAsync(DavStatusCode.MultiStatus, document, cancellationToken).ConfigureAwait(false);
    }

    private static XElement BuildResponseElement(IStoreItem item, Dictionary<XName, PropertyResult> propertyValues, string pathBase)
    {
        var response = new XElement(XmlNames.Response);
        response.Add(new XElement(XmlNames.Href, $"{pathBase}{item.Uri.AbsolutePath}"));

        foreach (var statusGrouping in propertyValues.GroupBy(x => x.Value.StatusCode))
        {
            var propStat = new XElement(XmlNames.PropertyStatus);
            var prop = new XElement(XmlNames.Property);

            foreach (var property in statusGrouping)
            {
                prop.Add(new XElement(property.Key, property.Value.Value));
            }

            propStat.Add(prop);
            propStat.Add(new XElement(XmlNames.Status, $"HTTP/1.1 {(int)statusGrouping.Key} {statusGrouping.Key.GetDisplayName()}"));

            response.Add(propStat);
        }

        return response;
    }

    private static async Task AddItemsRecursive(
        IStoreCollection collection,
        Depth depth,
        int iteration,
        ICollection<IStoreItem> results,
        CancellationToken cancellationToken = default)
    {
        results.Add(collection);

        if (iteration >= (int)depth && depth != Depth.Infinity)
            return;

        var items = await collection.GetItemsAsync(cancellationToken);

        // Single pass: process collections recursively, add non-collections directly
        foreach (var item in items)
        {
            if (item is IStoreCollection subCollection)
            {
                await AddItemsRecursive(subCollection, depth, iteration + 1, results, cancellationToken);
            }
            else
            {
                results.Add(item);
            }
        }
    }

    private async Task<Dictionary<XName, PropertyResult>> GetPropertiesAsync(
        IStoreItem item,
        PropFindRequest request,
        CancellationToken cancellationToken = default)
    {
        if (request.OnlyPropertyNames)
        {
            var propertyNames = await PropertyManager.GetPropertyNamesAsync(item, cancellationToken);
            return propertyNames.ToDictionary(x => x, _ => new PropertyResult(DavStatusCode.Ok));
        }

        var propertyValues = new Dictionary<XName, PropertyResult>();
        // Use HashSet for O(1) lookup instead of List with All() O(n) check
        var properties = new HashSet<XName>();

        // add all non-expensive properties
        if (request.AllProperties)
        {
            var propertyNames = await PropertyManager.GetPropertyNamesAsync(item, cancellationToken);
            foreach (var propertyName in propertyNames)
            {
                var propertyMetadata = PropertyManager.GetPropertyMetadata(item, propertyName);
                if (propertyMetadata == null || !propertyMetadata.Expensive)
                    properties.Add(propertyName);
            }
        }

        // this will also contain properties which are included explicitly
        // HashSet.Add returns false if already exists, so no need for Contains check
        foreach (var propertyName in request.Properties)
        {
            properties.Add(propertyName);
        }

        foreach (var propertyName in properties)
        {
            try
            {
                var propertyValue = await PropertyManager.GetPropertyAsync(item, propertyName, cancellationToken);
                propertyValues.Add(propertyName, propertyValue);
            }
            catch
            {
                propertyValues.Add(propertyName, new PropertyResult(DavStatusCode.InternalServerError));
            }
        }

        return propertyValues;
    }

    private async Task<PropFindRequest> GetRequestedPropertiesAsync(HttpContext context, CancellationToken cancellationToken = default)
    {
        var document = await context.ReadDocumentAsync(cancellationToken);
        if (document == null)
        {
            return new PropFindRequest(
                Array.Empty<XName>(),
                false,
                true);
        }

        var propfind = document.Element(XmlNames.PropertyFind);
        if (propfind == null)
        {
            return new PropFindRequest(
                Array.Empty<XName>(),
                false,
                true);
        }

        var propElement = propfind.Element(XmlNames.Property);
        var allProp = propfind.Element(XmlNames.AllProperties);
        var propNames = propfind.Element(XmlNames.PropertyName);
        var include = propfind.Element(XmlNames.Include);

        // Collect property names directly into array to avoid intermediate allocations
        var propertyNames = new List<XName>();
        if (propElement != null)
        {
            foreach (var element in propElement.Elements())
            {
                propertyNames.Add(element.Name);
            }
        }

        if (include != null)
        {
            foreach (var element in include.Elements())
            {
                propertyNames.Add(element.Name);
            }
        }

        return new PropFindRequest(
            propertyNames.Count > 0 ? propertyNames.ToArray() : Array.Empty<XName>(),
            propNames != null,
            allProp != null);
    }

    private record PropFindRequest(
        IReadOnlyList<XName> Properties,
        bool OnlyPropertyNames,
        bool AllProperties);
}