using System.Text.Json;
using System.Security.Cryptography;
using System.Text;

namespace SeldonEngine.Corpus
{
    // -------------------------------------------------------------------------
    // SELDON ENGINE — Layer 1: Aesthetic Corpus Ingest
    // Source: Metropolitan Museum of Art Open Access API
    // https://metmuseum.github.io/
    //
    // Socratic precondition (must survive before becoming invariant):
    //   Q: Does date-stamped aesthetic metadata constitute a civilizational signal?
    //   Q: Is ornamentation density measurable from museum metadata alone?
    //   Q: Does Western museum curation bias invalidate cross-cultural claims?
    //   Q: What would falsify the claim that aesthetic complexity tracks Kondratiev phase?
    // -------------------------------------------------------------------------

    public class MetMuseumIngestPipeline
    {
        private readonly HttpClient _http;
        private readonly string _outputPath;
        private readonly IProgress<IngestProgress>? _progress;

        private const string BaseUrl       = "https://collectionapi.metmuseum.org/public/collection/v1";
        private const int    RateLimitMs   = 500;  // 2 req/s — fewer retries, steadier
        private const int    MaxConcurrent = 1;    // serial to respect rate limit cleanly
        private const int    RetryCount    = 3;

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
            _http.Timeout = TimeSpan.FromSeconds(30);
            _http.DefaultRequestHeaders.Add(
                "User-Agent", "SeldonEngine/1.0 (civilizational-pattern-research)");

            Directory.CreateDirectory(outputPath);
        }

        // -----------------------------------------------------------------
        // Entry point — full corpus ingest
        // Recommended first run: departmentId = 11 (European Paintings)
        // This gives ~2,500 date-stamped works 1400–1900, cleanest signal
        // -----------------------------------------------------------------
        public async Task IngestAsync(
            int?  departmentId   = null,
            int?  startYear      = null,
            int?  endYear        = null,
            bool  hasImages      = true,
            CancellationToken ct = default)
        {
            Report(0, 0, "Fetching object IDs from Met API...");

            var ids = await FetchObjectIdsAsync(departmentId, hasImages, ct);

            Report(0, ids.Count, $"Found {ids.Count} objects. Beginning ingest...");

            var semaphore = new SemaphoreSlim(MaxConcurrent);
            var tasks     = new List<Task>();
            var processed = 0;
            var written   = 0;

            // Output stream — newline-delimited JSON for streaming processing
            var outFile = Path.Combine(
                _outputPath, $"met_corpus_{DateTime.UtcNow:yyyyMMdd_HHmmss}.ndjson");

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
                            Report(p, ids.Count, $"Processed {p}/{ids.Count} — written {written}");

                        // FIX: delay INSIDE try, BEFORE semaphore.Release()
                        // Previously the delay was after Release(), meaning the next task
                        // started immediately and requests hammered the API with no throttle.
                        await Task.Delay(RateLimitMs, ct);
                    }
                    finally
                    {
                        semaphore.Release();
                    }
                }, ct));
            }

            await Task.WhenAll(tasks);

            Report(ids.Count, ids.Count,
                $"Ingest complete. {written} records written to {outFile}");
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

            // Kondratiev relevant date range — industrial era waves 1–6
            url += "dateBegin=1700&dateEnd=2025";

            Report(0, 0, $"API URL: {url}");

            for (var attempt = 0; attempt < RetryCount; attempt++)
            {
                try
                {
                    Report(0, 0, $"HTTP GET attempt {attempt + 1}/{RetryCount}...");
                    var response = await _http.GetStringAsync(url, ct);
                    Report(0, 0, $"Response: {response.Length} chars");

                    var envelope = JsonSerializer.Deserialize<ObjectListEnvelope>(response);
                    var count    = envelope?.ObjectIDs?.Count ?? 0;
                    Report(0, 0, $"Parsed {count} object IDs");

                    return envelope?.ObjectIDs ?? [];
                }
                catch (Exception ex)
                {
                    Report(0, 0, $"ERROR: {ex.GetType().Name}: {ex.Message}");
                    if (attempt < RetryCount - 1)
                        await Task.Delay(1000 * (attempt + 1), ct);
                }
            }

            Report(0, 0, "FAILED: All retries exhausted");
            return [];
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
                    var url  = $"{BaseUrl}/objects/{id}";
                    if (id % 100 == 0) Console.WriteLine($"[DEBUG] Fetching object {id}");

                    var json = await _http.GetStringAsync(url, ct);
                    var obj  = JsonSerializer.Deserialize<MetObject>(json);

                    if (obj is null)
                    {
                        if (id % 100 == 0) Console.WriteLine($"[DEBUG] Object {id} deserialized as null");
                        return null;
                    }

                    // Parse year — Met uses string dates, often approximate
                    var year = ParseYear(obj.ObjectDate, obj.ObjectBeginDate, obj.ObjectEndDate);

                    if (id % 100 == 0)
                        Console.WriteLine(
                            $"[DEBUG] Object {id}: date='{obj.ObjectDate}', parsedYear={year}");

                    // Apply year filter if specified
                    if (startYear.HasValue && year.HasValue && year.Value < startYear.Value)
                    {
                        if (id % 100 == 0)
                            Console.WriteLine(
                                $"[DEBUG] Object {id} filtered: year {year} < {startYear}");
                        return null;
                    }
                    if (endYear.HasValue && year.HasValue && year.Value > endYear.Value)
                    {
                        if (id % 100 == 0)
                            Console.WriteLine(
                                $"[DEBUG] Object {id} filtered: year {year} > {endYear}");
                        return null;
                    }

                    var isReligious = ClassifyReligious(obj);
                    var merkleHash  = ComputeMerkleHash(
                        obj.ObjectId.ToString(), obj.Title ?? "", year?.ToString() ?? "");

                    return new AestheticRecord
                    {
                        MerkleHash        = merkleHash,
                        SourceId          = obj.ObjectId.ToString(),
                        Source            = "MetMuseum",
                        YearCreated       = year,
                        YearAcquired      = ParseYear(obj.AccessionYear),
                        Period            = obj.Period         ?? string.Empty,
                        Dynasty           = obj.Dynasty        ?? string.Empty,
                        Domain            = MapDomain(obj.Department ?? string.Empty),
                        Medium            = obj.Medium         ?? string.Empty,
                        Classification    = obj.Classification ?? string.Empty,
                        Department        = obj.Department     ?? string.Empty,
                        Culture           = obj.Culture        ?? string.Empty,
                        Country           = obj.Country        ?? string.Empty,
                        Region            = obj.Region         ?? string.Empty,
                        IsHighlight       = obj.IsHighlight,
                        HasImage          = !string.IsNullOrEmpty(obj.PrimaryImage),
                        Dimensions        = obj.Dimensions     ?? string.Empty,
                        Title             = obj.Title          ?? string.Empty,
                        ArtistName        = obj.ArtistDisplayName  ?? string.Empty,
                        ArtistNationality = obj.ArtistNationality  ?? string.Empty,
                        ArtistBeginDate   = obj.ArtistBeginDate    ?? string.Empty,
                        IsReligious       = isReligious,
                        RawJson           = json,
                        IngestedAt        = DateTime.UtcNow
                    };
                }
                catch (HttpRequestException ex)
                    when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
                {
                    // Object deleted or moved — not an error, do not retry
                    return null;
                }
                catch (HttpRequestException ex)
                    when (ex.StatusCode == System.Net.HttpStatusCode.TooManyRequests ||
                          ex.StatusCode == System.Net.HttpStatusCode.ServiceUnavailable)
                {
                    // Explicit throttle signal from the API — back off hard
                    var wait = 10000 * (attempt + 1);
                    Console.WriteLine(
                        $"[THROTTLE] Object {id} — HTTP {(int?)ex.StatusCode}, " +
                        $"backing off {wait}ms before retry {attempt + 1}/{RetryCount}");
                    if (attempt < RetryCount - 1)
                        await Task.Delay(wait, ct);
                }
                catch (HttpRequestException ex)
                    when (ex.StatusCode == System.Net.HttpStatusCode.Forbidden)
                {
                    // 403 typically means IP-level block after burst — back off hard
                    var wait = 15000 * (attempt + 1);
                    Console.WriteLine(
                        $"[BLOCKED] Object {id} — 403 Forbidden, " +
                        $"backing off {wait}ms before retry {attempt + 1}/{RetryCount}");
                    if (attempt < RetryCount - 1)
                        await Task.Delay(wait, ct);
                }
                catch (Exception ex)
                {
                    Console.WriteLine(
                        $"[DEBUG] Object {id} error: {ex.GetType().Name}: {ex.Message}");
                    if (attempt < RetryCount - 1)
                        await Task.Delay(2000 * (attempt + 1), ct);
                }
            }

            return null;
        }

        // -----------------------------------------------------------------
        // Merkle hash — content-addressed identity for DAG layer
        // Deterministic: identical content = identical hash = deduplication
        // -----------------------------------------------------------------
        private static string ComputeMerkleHash(params string[] components)
        {
            var content = string.Join("|", components);
            var bytes   = SHA256.HashData(Encoding.UTF8.GetBytes(content));
            return Convert.ToHexString(bytes).ToLower();
        }

        // -----------------------------------------------------------------
        // Year parsing — Met dates are messy strings
        // e.g. "ca. 1750", "1750–60", "18th century", "before 1800"
        // Socratic probe: does year imprecision corrupt the Kondratiev signal?
        // Answer: at 50yr wave scale, ±10 years is acceptable noise
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
            if (dateStr.Contains('–') || dateStr.Contains('-'))
            {
                var sep   = dateStr.Contains('–') ? '–' : '-';
                var parts = dateStr.Split(sep, 2);
                if (int.TryParse(parts[0].Trim(), out var y1) &&
                    int.TryParse(parts[1].Trim(), out var y2))
                    return (y1 + y2) / 2;
            }

            // Direct parse
            if (int.TryParse(dateStr.Trim(), out var year) && year > 0 && year < 2100)
                return year;

            // Century approximation — coarse but valid at Kondratiev scale
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
        // This is a first-pass proxy — vector layer will refine
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
        // Domain mapping — Art / Architecture / Music / Other
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
}