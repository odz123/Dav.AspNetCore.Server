using System.Diagnostics.CodeAnalysis;

namespace Dav.AspNetCore.Server.Http.Headers;

public class IfHeaderValue
{
    /// <summary>
    /// Initializes a new <see cref="IfHeaderValue"/> class.
    /// </summary>
    /// <param name="resourceConditions">The resource conditions.</param>
    public IfHeaderValue(IEnumerable<IfHeaderValueCondition> resourceConditions)
    {
        ArgumentNullException.ThrowIfNull(resourceConditions, nameof(resourceConditions));
        Conditions = new List<IfHeaderValueCondition>(resourceConditions).AsReadOnly();
    }
    
    /// <summary>
    /// Gets the resource conditions.
    /// </summary>
    public IReadOnlyCollection<IfHeaderValueCondition> Conditions { get; }

    /// <summary>
    /// Parses the input.
    /// </summary>
    /// <param name="input">The input.</param>
    /// <returns>The if header value.</returns>
    /// <exception cref="FormatException"></exception>
    public static IfHeaderValue Parse(string input)
    {
        if (TryParse(input, out var parsedValue))
            return parsedValue;

        throw new FormatException("The If header could not be parsed.");
    }

    /// <summary>
    /// Try to parse the input.
    /// </summary>
    /// <param name="input">The input.</param>
    /// <param name="parsedValue">The if header value.</param>
    /// <returns>True on success, otherwise false.</returns>
    public static bool TryParse(string? input, [NotNullWhen(true)] out IfHeaderValue? parsedValue)
    {
        parsedValue = null;

        if (string.IsNullOrWhiteSpace(input))
            return false;

        // we either start with a tagged list or an untagged list
        // the untagged list will always refer to the requested resource while
        // an tagged list specifies arbitrary resources to match.

        var inputSpan = input.AsSpan();
        var currentResourceTag = string.Empty;
        var resources = new List<ResourceTag>();
        var conditions = new List<Condition>();
        var stateTokens = new List<IfHeaderValueStateToken>();
        var tags = new List<IfHeaderValueEntityTag>();

        for (var i = 0; i < inputSpan.Length; i++)
        {
            if (inputSpan[i] == ' ')
                continue;

            if (inputSpan[i] == '<')
            {
                var closingIndex = inputSpan.Slice(i).IndexOf('>');
                if (closingIndex < 0)
                    return false;
                closingIndex += i; // Adjust for slice offset

                var resourceTag = inputSpan.Slice(i + 1, closingIndex - i - 1).ToString();

                // Save any pending conditions for the previous resource tag
                // This handles both tagged and untagged (empty) resource tags
                if (conditions.Count > 0)
                {
                    resources.Add(new ResourceTag(currentResourceTag, conditions.ToArray()));
                    conditions.Clear();
                }

                currentResourceTag = resourceTag;

                i = closingIndex;
                continue;
            }

            if (inputSpan[i] == '(')
            {
                var closingIndex = inputSpan.Slice(i).IndexOf(')');
                if (closingIndex < 0)
                    return false;
                closingIndex += i; // Adjust for slice offset

                var conditionSpan = inputSpan.Slice(i + 1, closingIndex - i - 1);

                var negate = false;
                for (var j = 0; j < conditionSpan.Length; j++)
                {
                    if (conditionSpan[j] == ' ')
                        continue;

                    // state token
                    if (conditionSpan[j] == '<')
                    {
                        var closeStateTokenIndex = conditionSpan.Slice(j).IndexOf('>');
                        if (closeStateTokenIndex < 0)
                            return false;
                        closeStateTokenIndex += j; // Adjust for slice offset

                        var stateToken = conditionSpan.Slice(j + 1, closeStateTokenIndex - j - 1).ToString();

                        stateTokens.Add(new IfHeaderValueStateToken(stateToken, negate));

                        negate = false;
                        j = closeStateTokenIndex;
                        continue;
                    }

                    // not - use direct character comparison instead of Substring
                    if ((conditionSpan[j] == 'N' || conditionSpan[j] == 'n') &&
                        conditionSpan.Length >= j + 3)
                    {
                        var notSpan = conditionSpan.Slice(j, 3);
                        if (notSpan.Equals("NOT", StringComparison.OrdinalIgnoreCase))
                        {
                            negate = true;
                            // Move to the last character of "NOT" (j+2), loop will increment to j+3
                            j += 2;
                            continue;
                        }
                    }

                    // etag
                    if (conditionSpan[j] == '[')
                    {
                        var closingEtagIndex = conditionSpan.Slice(j).IndexOf(']');
                        if (closingEtagIndex < 0)
                            return false;
                        closingEtagIndex += j; // Adjust for slice offset

                        var etagSpan = conditionSpan.Slice(j + 1, closingEtagIndex - j - 1);
                        if (!etagSpan.EndsWith("\"") || (!etagSpan.StartsWith("\"") && !etagSpan.StartsWith("W/\"")))
                            return false;

                        var isWeak = etagSpan.StartsWith("W/");
                        var etagValue = isWeak
                            ? etagSpan.Slice(3, etagSpan.Length - 4).ToString()
                            : etagSpan.Slice(1, etagSpan.Length - 2).ToString();

                        tags.Add(new IfHeaderValueEntityTag(etagValue, isWeak, negate));

                        negate = false;
                        j = closingEtagIndex;
                        continue;
                    }

                    return false;
                }

                conditions.Add(new Condition(
                    stateTokens.ToArray(),
                    tags.ToArray()));

                stateTokens.Clear();
                tags.Clear();

                i = closingIndex;
                continue;
            }

            return false;
        }

        if (resources.All(x => x.Name != currentResourceTag))
            resources.Add(new ResourceTag(currentResourceTag, conditions.ToArray()));

        var resourceConditions = new List<IfHeaderValueCondition>(resources.Count * 2);
        foreach (var resourceTag in resources)
        {
            foreach (var condition in resourceTag.Conditions)
            {
                Uri? conditionUri = null;
                if (!string.IsNullOrWhiteSpace(resourceTag.Name))
                {
                    if (!Uri.TryCreate(resourceTag.Name.TrimEnd('/'), UriKind.RelativeOrAbsolute, out conditionUri))
                        return false;
                }
                resourceConditions.Add(new IfHeaderValueCondition(conditionUri, condition.Tokens, condition.Tags));
            }
        }

        parsedValue = new IfHeaderValue(resourceConditions);
        return true;
    }

    private record ResourceTag(string Name, Condition[] Conditions);

    private record Condition(IfHeaderValueStateToken[] Tokens, IfHeaderValueEntityTag[] Tags);
}