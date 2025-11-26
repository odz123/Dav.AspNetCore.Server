using System.Diagnostics.CodeAnalysis;

namespace Dav.AspNetCore.Server.Http.Headers;

public class DepthHeaderValue
{
    /// <summary>
    /// Initializes a new <see cref="DepthHeaderValue"/> class.
    /// </summary>
    /// <param name="depth">The depth.</param>
    public DepthHeaderValue(Depth depth)
    {
        Depth = depth;
    }
    
    /// <summary>
    /// Gets the depth.
    /// </summary>
    public Depth Depth { get; }
    
    /// <summary>
    /// Parses the input.
    /// </summary>
    /// <param name="input">The input.</param>
    /// <returns>The depth header value.</returns>
    /// <exception cref="FormatException"></exception>
    public static DepthHeaderValue Parse(string input)
    {
        if (TryParse(input, out var parsedValue))
            return parsedValue;

        throw new FormatException("The Depth-Header could not be parsed.");
    }

    /// <summary>
    /// Try to parse the input.
    /// </summary>
    /// <param name="input">The input.</param>
    /// <param name="parsedValue">The depth header value.</param>
    /// <returns>True on success, otherwise false.</returns>
    public static bool TryParse(string? input, [NotNullWhen(true)] out DepthHeaderValue? parsedValue)
    {
        parsedValue = null;

        if (string.IsNullOrWhiteSpace(input))
            return false;

        var trimmedInput = input.Trim();
        if (trimmedInput == "0")
        {
            parsedValue = new DepthHeaderValue(Depth.None);
            return true;
        }
        if (trimmedInput == "1")
        {
            parsedValue = new DepthHeaderValue(Depth.One);
            return true;
        }
        if (trimmedInput.Equals("infinity", StringComparison.OrdinalIgnoreCase))
        {
            parsedValue = new DepthHeaderValue(Depth.Infinity);
            return true;
        }

        return false;
    }
}