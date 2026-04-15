using System.Text.Json.Serialization;

namespace SeldonEngine.Corpus;

public record MetTag
{
    [JsonPropertyName("term")]   public string? Term   { get; init; }
    [JsonPropertyName("AAT_URL")]public string? AatUrl { get; init; }
}