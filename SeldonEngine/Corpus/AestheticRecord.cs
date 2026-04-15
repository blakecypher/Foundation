namespace SeldonEngine.Corpus;

public record AestheticRecord
{
    // Identity
    public string MerkleHash    { get; init; } = string.Empty;
    public string SourceId      { get; init; } = string.Empty;
    public string Source        { get; init; } = "MetMuseum";

    // Temporal anchor � primary Kondratiev alignment axis
    public int?   YearCreated   { get; init; }
    public int?   YearAcquired  { get; init; }
    public string Period        { get; init; } = string.Empty;
    public string Dynasty       { get; init; } = string.Empty;

    // Domain classification
    public string Domain        { get; init; } = string.Empty; // Art / Music / Architecture
    public string Medium        { get; init; } = string.Empty;
    public string Classification { get; init; } = string.Empty;
    public string Department    { get; init; } = string.Empty;

    // Geographic signal � civilizational origin
    public string Culture       { get; init; } = string.Empty;
    public string Country       { get; init; } = string.Empty;
    public string Region        { get; init; } = string.Empty;

    // Aesthetic signal proxies � raw inputs to vector layer
    public bool   IsHighlight   { get; init; }  // Institutional prestige signal
    public bool   HasImage      { get; init; }  // Visual analysis availability
    public string Dimensions    { get; init; } = string.Empty; // Scale proxy
    public string Title         { get; init; } = string.Empty;
    public string ArtistName    { get; init; } = string.Empty;
    public string ArtistNationality { get; init; } = string.Empty;
    public string ArtistBeginDate   { get; init; } = string.Empty;

    // Sacred / secular marker � leading Kondratiev indicator
    public bool   IsReligious   { get; init; }

    // Raw JSON preserved for vector layer reprocessing
    public string RawJson       { get; init; } = string.Empty;

    // Ingestion metadata
    public DateTime IngestedAt  { get; init; } = DateTime.UtcNow;
}