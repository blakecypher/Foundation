using System.Text.Json;
using System.Text.Json.Serialization;
using System.Security.Cryptography;
using System.Text;

namespace SeldonEngine.Corpus
{
    // -------------------------------------------------------------------------
    // SELDON ENGINE � Layer 1: Aesthetic Corpus Ingest
    // Source: Metropolitan Museum of Art Open Access API
    // https://metmuseum.github.io/
    //
    // Socratic precondition (must survive before becoming invariant):
    //   Q: Does date-stamped aesthetic metadata constitute a civilizational signal?
    //   Q: Is ornamentation density measurable from museum metadata alone?
    //   Q: Does Western museum curation bias invalidate cross-cultural claims?
    //   Q: What would falsify the claim that aesthetic complexity tracks Kondratiev phase?
    // -------------------------------------------------------------------------

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

    public class MetMuseumIngestPipeline
    {
        private readonly HttpClient   _http;
        private readonly string       _outputPath;
        private readonly IProgress<IngestProgress>? _progress;

        private const string BaseUrl        = "https://collectionapi.metmuseum.org/public/collection/v1";
        private const int    RateLimitMs    = 200;   // Met API is permissive but be respectful
        private const int    MaxConcurrent  = 5;
        private const int    RetryCount     = 3;

        // Socratic probe: these keywords mark sacred/religious works
        // Q: Is this list complete? Q: Does absence of keyword = secular?
        // Q: What about works with ambiguous sacred-secular function?
        private static readonly HashSet<string> SacredMarkers = new(StringComparer.OrdinalIgnoreCase)
        {
            "religious", "sacred", "church", "cathedral", "temple", "mosque",
            "altar", "icon", "devotional", "liturgical", "bible", "torah",
            "quran", "saint", "madonna", "christ", "buddha", "deity", "ritual",
            "votive", "ex-voto", "reliquary", "chalice", "crucifix", "triptych"
        };

        public MetMuseumIngestPipeline(
            string outputPath,
            IProgress<IngestProgress>? progress = null)
        {
            _outputPath = outputPath;
            _progress   = progress;
            _http       = new HttpClient();
            _http.Timeout = TimeSpan.FromSeconds(30);  // ADD THIS LINE
            _http.DefaultRequestHeaders.Add("User-Agent", "SeldonEngine/1.0 (civilizational-pattern-research)");

            Directory.CreateDirectory(outputPath);
        }

        // -----------------------------------------------------------------
        // Entry point � full corpus ingest
        // Recommended first run: departmentId = 11 (European Paintings)
        // This gives ~2,500 date-stamped works 1400�1900, cleanest signal
        // -----------------------------------------------------------------
        public async Task IngestAsync(
            int?  departmentId     = null,
            int?  startYear        = null,
            int?  endYear          = null,
            bool  hasImages        = true,
            CancellationToken ct   = default)
        {
            Report(0, 0, "Fetching object IDs from Met API...");

            var ids = await FetchObjectIdsAsync(departmentId, hasImages, ct);

            Report(0, ids.Count, $"Found {ids.Count} objects. Beginning ingest...");

            var semaphore = new SemaphoreSlim(MaxConcurrent);
            var tasks     = new List<Task>();
            var processed = 0;
            var written   = 0;

            // Output stream � newline-delimited JSON for streaming processing
            var outFile = Path.Combine(_outputPath, $"met_corpus_{DateTime.UtcNow:yyyyMMdd_HHmmss}.ndjson");

            await using var writer = new StreamWriter(outFile, append: false, encoding: Encoding.UTF8);

            foreach (var id in ids)
            {
                await semaphore.WaitAsync(ct);

                tasks.Add(Task.Run(async () =>
                {
                    try
                    {
                        var record = await FetchAndMapObjectAsync(id, startYear, endYear, ct);

                        if (record is not null)
                        {
                            var json = JsonSerializer.Serialize(record);

                            lock (writer)
                            {
                                writer.WriteLine(json);
                                written++;
                            }
                        }

                        var p = Interlocked.Increment(ref processed);
                        if (p % 100 == 0)
                            Report(p, ids.Count, $"Processed {p}/{ids.Count} � written {written}");
                    }
                    finally
                    {
                        semaphore.Release();
                    }

                    await Task.Delay(RateLimitMs, ct);
                }, ct));
            }

            await Task.WhenAll(tasks);

            Report(ids.Count, ids.Count, $"Ingest complete. {written} records written to {outFile}");
        }

        // -----------------------------------------------------------------
        // Fetch all object IDs matching parameters
        // -----------------------------------------------------------------
        private async Task<List<int>> FetchObjectIdsAsync(
            int?  departmentId,
            bool  hasImages,
            CancellationToken ct)
        {
            var url = $"{BaseUrl}/objects?";

            if (departmentId.HasValue) url += $"departmentIds={departmentId}&";
            if (hasImages)             url += "hasImages=true&";

            // Kondratiev relevant date range � industrial era waves 1�6
            url += "dateBegin=1700&dateEnd=2025";

            Report(0, 0, $"API URL: {url}");

            for (var attempt = 0; attempt < RetryCount; attempt++)
            {
                try
                {
                    Report(0, 0, $"HTTP GET attempt {attempt + 1}/3...");
                    var response = await _http.GetStringAsync(url, ct);
                    Report(0, 0, $"Response: {response.Length} chars");

                    var envelope = JsonSerializer.Deserialize<ObjectListEnvelope>(response);
                    var count = envelope?.ObjectIDs?.Count ?? 0;
                    Report(0, 0, $"Parsed {count} object IDs");

                    return envelope?.ObjectIDs ?? new List<int>();
                }
                catch (Exception ex)
                {
                    Report(0, 0, $"ERROR: {ex.GetType().Name}: {ex.Message}");
                    if (attempt < RetryCount - 1)
                    {
                        await Task.Delay(1000 * (attempt + 1), ct);
                    }
                }
            }

            Report(0, 0, "FAILED: All retries exhausted");
            return new List<int>();
        }

        // -----------------------------------------------------------------
        // Fetch individual object and map to AestheticRecord
        // -----------------------------------------------------------------
        private async Task<AestheticRecord?> FetchAndMapObjectAsync(
            int id,
            int? startYear,
            int? endYear,
            CancellationToken ct)
        {
            for (var attempt = 0; attempt < RetryCount; attempt++)
            {
                try
                {
                    var json = await _http.GetStringAsync($"{BaseUrl}/objects/{id}", ct);
                    var obj  = JsonSerializer.Deserialize<MetObject>(json);

                    if (obj is null) return null;

                    // Parse year � Met uses string dates, often approximate
                    var year = ParseYear(obj.ObjectDate, obj.ObjectBeginDate, obj.ObjectEndDate);

                    // Apply year filter if specified
                    if (startYear.HasValue && year.HasValue && year.Value < startYear.Value) return null;
                    if (endYear.HasValue   && year.HasValue && year.Value > endYear.Value)   return null;

                    // Sacred/secular classification
                    // Socratic probe: this is a proxy, not ground truth
                    var isReligious = ClassifyReligious(obj);

                    // Compute Merkle hash � content-addressed identity
                    // Hash is deterministic: same object always same hash
                    // Enables deduplication across ingest runs
                    var merkleHash = ComputeMerkleHash(obj.ObjectID.ToString(), obj.Title ?? "", year?.ToString() ?? "");

                    return new AestheticRecord
                    {
                        MerkleHash          = merkleHash,
                        SourceId            = obj.ObjectID.ToString(),
                        Source              = "MetMuseum",
                        YearCreated         = year,
                        YearAcquired        = ParseYear(obj.AccessionYear),
                        Period              = obj.Period     ?? string.Empty,
                        Dynasty             = obj.Dynasty    ?? string.Empty,
                        Domain              = MapDomain(obj.Department ?? string.Empty),
                        Medium              = obj.Medium     ?? string.Empty,
                        Classification      = obj.Classification ?? string.Empty,
                        Department          = obj.Department ?? string.Empty,
                        Culture             = obj.Culture    ?? string.Empty,
                        Country             = obj.Country    ?? string.Empty,
                        Region              = obj.Region     ?? string.Empty,
                        IsHighlight         = obj.IsHighlight,
                        HasImage            = !string.IsNullOrEmpty(obj.PrimaryImage),
                        Dimensions          = obj.Dimensions ?? string.Empty,
                        Title               = obj.Title      ?? string.Empty,
                        ArtistName          = obj.ArtistDisplayName ?? string.Empty,
                        ArtistNationality   = obj.ArtistNationality ?? string.Empty,
                        ArtistBeginDate     = obj.ArtistBeginDate   ?? string.Empty,
                        IsReligious         = isReligious,
                        RawJson             = json,
                        IngestedAt          = DateTime.UtcNow
                    };
                }
                catch (HttpRequestException ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
                {
                    return null; // Object deleted or moved � not an error
                }
                catch (Exception ex) when (attempt < RetryCount - 1)
                {
                    await Task.Delay(500 * (attempt + 1), ct);
                }
            }

            return null;
        }

        // -----------------------------------------------------------------
        // Merkle hash � content-addressed identity for DAG layer
        // Deterministic: identical content = identical hash = deduplication
        // -----------------------------------------------------------------
        private static string ComputeMerkleHash(params string[] components)
        {
            var content = string.Join("|", components);
            var bytes   = SHA256.HashData(Encoding.UTF8.GetBytes(content));
            return Convert.ToHexString(bytes).ToLower();
        }

        // -----------------------------------------------------------------
        // Year parsing � Met dates are messy strings
        // e.g. "ca. 1750", "1750�60", "18th century", "before 1800"
        // Socratic probe: does year imprecision corrupt the Kondratiev signal?
        // Answer: at 50yr wave scale, �10 years is acceptable noise
        // -----------------------------------------------------------------
        private static int? ParseYear(string? dateStr, int? beginDate = null, int? endDate = null)
        {
            if (beginDate.HasValue && endDate.HasValue)
                return (beginDate.Value + endDate.Value) / 2;

            if (beginDate.HasValue) return beginDate.Value;
            if (endDate.HasValue)   return endDate.Value;

            if (string.IsNullOrWhiteSpace(dateStr)) return null;

            // Strip common prefixes
            dateStr = dateStr
                .Replace("ca.", "").Replace("c.", "").Replace("circa", "")
                .Replace("before", "").Replace("after", "").Replace("probably", "")
                .Trim();

            // Range: take midpoint
            if (dateStr.Contains('�') || dateStr.Contains('-'))
            {
                var sep  = dateStr.Contains('�') ? '�' : '-';
                var parts = dateStr.Split(sep, 2);
                if (int.TryParse(parts[0].Trim(), out var y1) &&
                    int.TryParse(parts[1].Trim(), out var y2))
                    return (y1 + y2) / 2;
            }

            // Direct parse
            if (int.TryParse(dateStr.Trim(), out var year) && year > 0 && year < 2100)
                return year;

            // Century approximation � coarse but valid at Kondratiev scale
            if (dateStr.Contains("century", StringComparison.OrdinalIgnoreCase))
            {
                if (dateStr.Contains("18th")) return 1750;
                if (dateStr.Contains("19th")) return 1850;
                if (dateStr.Contains("20th")) return 1950;
                if (dateStr.Contains("17th")) return 1650;
                if (dateStr.Contains("16th")) return 1550;
            }

            return null;
        }

        // -----------------------------------------------------------------
        // Sacred/secular classification
        // Socratic probe: is keyword matching sufficient?
        // This is a first-pass proxy � vector layer will refine
        // -----------------------------------------------------------------
        private static bool ClassifyReligious(MetObject obj)
        {
            var fields = new[]
            {
                obj.Title, obj.Classification, obj.Department,
                obj.Culture, obj.Medium, obj.Period
            };

            foreach (var field in fields)
            {
                if (string.IsNullOrEmpty(field)) continue;
                foreach (var marker in SacredMarkers)
                    if (field.Contains(marker, StringComparison.OrdinalIgnoreCase))
                        return true;
            }

            return false;
        }

        // -----------------------------------------------------------------
        // Domain mapping � Art / Architecture / Other
        // -----------------------------------------------------------------
        private static string MapDomain(string department) => department switch
        {
            var d when d.Contains("Architecture", StringComparison.OrdinalIgnoreCase) => "Architecture",
            var d when d.Contains("Drawings",     StringComparison.OrdinalIgnoreCase) => "Art",
            var d when d.Contains("Paintings",    StringComparison.OrdinalIgnoreCase) => "Art",
            var d when d.Contains("Prints",       StringComparison.OrdinalIgnoreCase) => "Art",
            var d when d.Contains("Sculpture",    StringComparison.OrdinalIgnoreCase) => "Art",
            var d when d.Contains("Musical",      StringComparison.OrdinalIgnoreCase) => "Music",
            _ => "Art"
        };

        private void Report(int current, int total, string message) =>
            _progress?.Report(new IngestProgress(current, total, message));
    }

    // -----------------------------------------------------------------
    // Met API response shapes
    // -----------------------------------------------------------------
    public record ObjectListEnvelope
    {
        [JsonPropertyName("total")]     public int        Total     { get; init; }
        [JsonPropertyName("objectIDs")] public List<int>? ObjectIDs { get; init; }
    }

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

    public record MetTag
    {
        [JsonPropertyName("term")]   public string? Term   { get; init; }
        [JsonPropertyName("AAT_URL")]public string? AatUrl { get; init; }
    }

    public record IngestProgress(int Current, int Total, string Message);
}
