using System.Text.Json.Serialization;

namespace SeldonEngine.Corpus;

public record MetObject
{
    [JsonPropertyName("objectID")]           public int     ObjectID          { get; init; }
    [JsonPropertyName("isHighlight")]        public bool    IsHighlight       { get; init; }
    [JsonPropertyName("accessionYear")]      public string? AccessionYear     { get; init; }
    [JsonPropertyName("isPublicDomain")]     public bool    IsPublicDomain    { get; init; }
    [JsonPropertyName("primaryImage")]       public string? PrimaryImage      { get; init; }
    [JsonPropertyName("department")]         public string? Department        { get; init; }
    [JsonPropertyName("objectName")]         public string? ObjectName        { get; init; }
    [JsonPropertyName("title")]              public string? Title             { get; init; }
    [JsonPropertyName("culture")]            public string? Culture           { get; init; }
    [JsonPropertyName("period")]             public string? Period            { get; init; }
    [JsonPropertyName("dynasty")]            public string? Dynasty           { get; init; }
    [JsonPropertyName("reign")]              public string? Reign             { get; init; }
    [JsonPropertyName("medium")]             public string? Medium            { get; init; }
    [JsonPropertyName("dimensions")]         public string? Dimensions        { get; init; }
    [JsonPropertyName("classification")]     public string? Classification    { get; init; }
    [JsonPropertyName("artistDisplayName")]  public string? ArtistDisplayName { get; init; }
    [JsonPropertyName("artistNationality")]  public string? ArtistNationality { get; init; }
    [JsonPropertyName("artistBeginDate")]    public string? ArtistBeginDate   { get; init; }
    [JsonPropertyName("artistEndDate")]      public string? ArtistEndDate     { get; init; }
    [JsonPropertyName("objectDate")]         public string? ObjectDate        { get; init; }
    [JsonPropertyName("objectBeginDate")]    public int?    ObjectBeginDate   { get; init; }
    [JsonPropertyName("objectEndDate")]      public int?    ObjectEndDate     { get; init; }
    [JsonPropertyName("country")]            public string? Country           { get; init; }
    [JsonPropertyName("region")]             public string? Region            { get; init; }
    [JsonPropertyName("subregion")]          public string? Subregion         { get; init; }
    [JsonPropertyName("tags")]               public List<MetTag>? Tags        { get; init; }
}