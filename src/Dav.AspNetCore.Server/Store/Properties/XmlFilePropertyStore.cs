using System.Collections.Concurrent;
using System.Xml;
using System.Xml.Linq;

namespace Dav.AspNetCore.Server.Store.Properties;

public class XmlFilePropertyStore : IPropertyStore
{
    private const string Namespace = "https://github.com/ThuCommix/Dav.AspNetCore.Server";
    private static readonly XName PropertyStore = XName.Get("PropertyStore", Namespace);
    private static readonly XName Property = XName.Get("Property", Namespace);

    private readonly XmlFilePropertyStoreOptions options;
    private readonly string normalizedRootPath;
    private readonly ConcurrentDictionary<IStoreItem, ConcurrentDictionary<XName, PropertyData>> propertyCache = new();
    private readonly ConcurrentDictionary<IStoreItem, bool> writeLookup = new();

    /// <summary>
    /// Initializes a new <see cref="XmlFilePropertyStore"/> class.
    /// </summary>
    /// <param name="options">The xml file property store options.</param>
    public XmlFilePropertyStore(XmlFilePropertyStoreOptions options)
    {
        ArgumentNullException.ThrowIfNull(options, nameof(options));
        this.options = options;
        // Normalize the root path once at construction and ensure it ends with separator
        normalizedRootPath = Path.GetFullPath(options.RootPath).TrimEnd(Path.DirectorySeparatorChar) + Path.DirectorySeparatorChar;
    }

    /// <summary>
    /// Safely combines root path with URI path and validates against path traversal.
    /// </summary>
    /// <param name="uri">The URI to combine with root path.</param>
    /// <param name="suffix">Optional suffix to append (e.g., ".xml").</param>
    /// <returns>The validated full path.</returns>
    /// <exception cref="InvalidOperationException">Thrown when path traversal is detected.</exception>
    private string GetSafePath(Uri uri, string suffix = "")
    {
        var combinedPath = Path.Combine(options.RootPath, uri.LocalPath.TrimStart('/') + suffix);
        var fullPath = Path.GetFullPath(combinedPath);

        // Ensure the resolved path is within the root directory
        if (!fullPath.StartsWith(normalizedRootPath, StringComparison.OrdinalIgnoreCase) &&
            !fullPath.Equals(normalizedRootPath.TrimEnd(Path.DirectorySeparatorChar), StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException("Access denied: Path traversal detected.");
        }

        return fullPath;
    }

    /// <summary>
    /// Commits the property store changes async.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns></returns>
    public async ValueTask SaveChangesAsync(CancellationToken cancellationToken = default)
    {
        foreach (var entry in propertyCache)
        {
            if (!writeLookup.ContainsKey(entry.Key))
                continue;
            
            var propertyStore = new XElement(PropertyStore);
            var document = new XDocument(new XDeclaration("1.0", "utf-8", null),
                propertyStore);

            foreach (var propertyData in entry.Value)
            {
                propertyStore.Add(new XElement(Property, new XElement(propertyData.Value.Name, propertyData.Value.CurrentValue)));
            }
            
            var xmlFilePath = GetSafePath(entry.Key.Uri, ".xml");
            var fileInfo = new FileInfo(xmlFilePath);
            if (fileInfo.Directory?.Exists == false)
                fileInfo.Directory.Create();
            
            await using var fileStream = File.OpenWrite(xmlFilePath);
            await document.SaveAsync(fileStream, SaveOptions.None, cancellationToken);
        }
    }

    /// <summary>
    /// Deletes all properties of the specified item async.
    /// </summary>
    /// <param name="item">The store item.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns></returns>
    public ValueTask DeletePropertiesAsync(
        IStoreItem item, 
        CancellationToken cancellationToken = default)
    {
        var xmlFilePath = GetSafePath(item.Uri, ".xml");
        if (File.Exists(xmlFilePath))
            File.Delete(xmlFilePath);

        propertyCache.Remove(item);
        
        return ValueTask.CompletedTask;
    }

    /// <summary>
    /// Copies all properties of the specified item to the destination async.
    /// </summary>
    /// <param name="source">The source store item.</param>
    /// <param name="destination">The destination store item.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns></returns>
    public ValueTask CopyPropertiesAsync(
        IStoreItem source, 
        IStoreItem destination, 
        CancellationToken cancellationToken = default)
    {
        var sourceXmlFilePath = GetSafePath(source.Uri, ".xml");
        var destinationXmlFilePath = GetSafePath(destination.Uri, ".xml");
        
        if (File.Exists(sourceXmlFilePath))
        {
            var fileInfo = new FileInfo(destinationXmlFilePath);
            if (fileInfo.Directory?.Exists == false)
                fileInfo.Directory.Create();

            File.Copy(sourceXmlFilePath, destinationXmlFilePath, true);
        }

        if (propertyCache.TryGetValue(source, out var propertyMap))
        {
            propertyCache[destination] = propertyMap.ToDictionary(x => x.Key, x => x.Value);
        }

        return ValueTask.CompletedTask;
    }

    /// <summary>
    /// Sets a property of the specified item async.
    /// </summary>
    /// <param name="item">The store item.</param>
    /// <param name="propertyName">The property name.</param>
    /// <param name="propertyMetadata">The property metadata.</param>
    /// <param name="isRegistered">A value indicating whether the property is registered.</param>
    /// <param name="propertyValue">The property value.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns></returns>
    public async ValueTask<bool> SetPropertyAsync(
        IStoreItem item, 
        XName propertyName, 
        PropertyMetadata propertyMetadata,
        bool isRegistered,
        object? propertyValue,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(item, nameof(item));
        ArgumentNullException.ThrowIfNull(propertyName, nameof(propertyName));
        
        if (!propertyCache.TryGetValue(item, out var propertyMap))
        {
            await GetPropertiesAsync(item, cancellationToken);
            propertyMap = propertyCache[item];
        }

        var propertyExists = propertyMap.ContainsKey(propertyName);
        if (propertyExists || options.AcceptCustomProperties || isRegistered)
        {
            propertyMap[propertyName] = new PropertyData(propertyName, propertyValue);
            writeLookup[item] = true;
            return true;
        }

        return false;
    }

    /// <summary>
    /// Gets the properties of the specified item async.
    /// </summary>
    /// <param name="item">The store item.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A list of stored properties.</returns>
    public async ValueTask<IReadOnlyCollection<PropertyData>> GetPropertiesAsync(
        IStoreItem item, 
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(item, nameof(item));
        
        if (propertyCache.TryGetValue(item, out var propertyMap))
            return propertyMap.Values;
        
        var xmlFilePath = GetSafePath(item.Uri, ".xml");
        if (!File.Exists(xmlFilePath))
        {
            propertyCache[item] = new ConcurrentDictionary<XName, PropertyData>();
            return Array.Empty<PropertyData>();
        }

        var fileStream = File.OpenRead(xmlFilePath);
        var propertyDataList = new List<PropertyData>();
        
        try
        {
            var document = await XDocument.LoadAsync(fileStream, LoadOptions.None, cancellationToken);
            var propertyStore = document.Element(PropertyStore);
            var properties = propertyStore?.Elements(Property).Select(x => x.Elements().First());
            if (properties == null)
            {
                propertyCache[item] = new Dictionary<XName, PropertyData>();
                return propertyDataList;
            }

            foreach (var property in properties)
            {
                object? propertyValue = null;
                if (property.FirstNode != null)
                {
                    propertyValue = property.FirstNode.NodeType switch
                    {
                        XmlNodeType.Text => property.Value,
                        XmlNodeType.Element => property.Elements().ToArray(),
                        _ => propertyValue
                    };
                }

                var propertyData = new PropertyData(
                    property.Name,
                    propertyValue);
                
                propertyDataList.Add(propertyData);
            }
        }
        catch
        {
            propertyCache[item] = new ConcurrentDictionary<XName, PropertyData>();
            return propertyDataList;
        }
        finally
        {
            await fileStream.DisposeAsync();
        }

        var newCache = new ConcurrentDictionary<XName, PropertyData>();
        foreach (var propertyData in propertyDataList)
        {
            newCache[propertyData.Name] = propertyData;
        }
        propertyCache[item] = newCache;
        
        return propertyDataList;
    }
}