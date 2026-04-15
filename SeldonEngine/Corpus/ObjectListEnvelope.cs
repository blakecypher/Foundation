using System.Text.Json.Serialization;

namespace SeldonEngine.Corpus;

public record ObjectListEnvelope
{
    [JsonPropertyName("total")]     public int        Total     { get; init; }
    [JsonPropertyName("objectIDs")] public List<int>? ObjectIDs { get; init; }
}