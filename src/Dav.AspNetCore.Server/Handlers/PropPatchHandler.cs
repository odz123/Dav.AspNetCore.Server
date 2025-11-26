using System.Xml;
using System.Xml.Linq;

namespace Dav.AspNetCore.Server.Handlers;

internal class PropPatchHandler : RequestHandler
{
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

        var requestDocument = await Context.ReadDocumentAsync(cancellationToken);
        if (requestDocument == null)
        {
            Context.SetResult(DavStatusCode.BadRequest);
            return;
        }

        var propertyUpdate = requestDocument.Element(XmlNames.PropertyUpdate);
        if (propertyUpdate == null)
        {
            Context.SetResult(DavStatusCode.BadRequest);
            return;
        }

        var results = new Dictionary<XName, DavStatusCode>();

        // Process all <set> elements (there can be multiple per RFC 4918)
        foreach (var setElement in propertyUpdate.Elements(XmlNames.Set))
        {
            var props = setElement.Element(XmlNames.Property)?.Elements();
            if (props == null)
                continue;

            foreach (var element in props)
            {
                object? propertyValue = null;
                if (element.FirstNode != null)
                {
                    propertyValue = element.FirstNode.NodeType switch
                    {
                        XmlNodeType.Text => element.Value,
                        XmlNodeType.Element => element.Elements().ToArray(),
                        _ => propertyValue
                    };
                }

                var result = await PropertyManager.SetPropertyAsync(
                    Item,
                    element.Name,
                    propertyValue,
                    cancellationToken);

                results[element.Name] = result;
            }
        }

        // Process all <remove> elements (there can be multiple per RFC 4918)
        foreach (var removeElement in propertyUpdate.Elements(XmlNames.Remove))
        {
            var props = removeElement.Element(XmlNames.Property)?.Elements();
            if (props == null)
                continue;

            foreach (var element in props)
            {
                var result = await PropertyManager.SetPropertyAsync(
                    Item,
                    element.Name,
                    null,
                    cancellationToken);

                results[element.Name] = result;
            }
        }

        var href = new XElement(XmlNames.Href, $"{Context.Request.PathBase.ToUriComponent()}{Item.Uri.AbsolutePath}");
        var response = new XElement(XmlNames.Response, href);
        var multiStatus = new XElement(XmlNames.MultiStatus, response);
        var document = new XDocument(
            new XDeclaration("1.0", "utf-8", null),
            multiStatus);

        foreach (var statusGroup in results.GroupBy(x => x.Value))
        {
            var status = new XElement(XmlNames.Status, $"HTTP/1.1 {(int)statusGroup.Key} {statusGroup.Key.GetDisplayName()}");
            var prop = new XElement(XmlNames.Property);
            var propStat = new XElement(XmlNames.PropertyStatus, prop, status);

            foreach (var propertyName in statusGroup)
            {
                prop.Add(new XElement(propertyName.Key));
            }
            
            response.Add(propStat);
        }
        
        await Context.WriteDocumentAsync(DavStatusCode.MultiStatus, document, cancellationToken);
    }
}