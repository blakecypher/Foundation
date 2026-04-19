using System.Text.Json;
using System.Security.Cryptography;
using System.Text;

namespace SeldonEngine.Corpus
{
    // -------------------------------------------------------------------------
    // SELDON ENGINE — Layer 1: Aesthetic Corpus Ingest
    // Source-agnostic pipeline. Concrete sources implement ICorpusSource.
    //
    // Socratic precondition (must survive before becoming invariant):
    //   Q: Does date-stamped aesthetic metadata constitute a civilizational signal?
    //   Q: Is ornamentation density measurable from museum metadata alone?
    //   Q: Does Western museum curation bias invalidate cross-cultural claims?
    //   Q: What would falsify the claim that aesthetic complexity tracks Kondratiev phase?
    // -------------------------------------------------------------------------

    public class IngestPipeline
    {
        private readonly string _outputPath;
        private readonly IProgress<IngestProgress>? _progress;
        private readonly ICorpusSource _source;

        private const int MaxConcurrent = 1;    // serial to respect rate limits
        private const int RetryCount    = 3;

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

        public IngestPipeline(
            ICorpusSource source,
            string outputPath,
            IProgress<IngestProgress>? progress = null)
        {
            _source     = source;
            _outputPath = outputPath;
            _progress   = progress;
            var http = new HttpClient();
            http.Timeout = TimeSpan.FromSeconds(30);
            http.DefaultRequestHeaders.Add(
                "User-Agent", "SeldonEngine/1.0 (civilizational-pattern-research)");

            Directory.CreateDirectory(outputPath);
        }

        // -----------------------------------------------------------------
        // Entry point — full corpus ingest using the configured source
        // -----------------------------------------------------------------
        public async Task IngestAsync(
            int?  startYear      = null,
            int?  endYear        = null,
            CancellationToken ct = default)
        {
            Report(0, 0, $"Fetching object IDs from {_source.Name}...");

            var ids = await _source.FetchObjectIdsAsync(startYear, endYear, ct);

            Report(0, ids.Count, $"Found {ids.Count} objects. Beginning ingest...");

            var semaphore = new SemaphoreSlim(MaxConcurrent);
            var tasks     = new List<Task>();
            var processed = 0;
            var written   = 0;

            // Output stream — newline-delimited JSON for streaming processing
            var sourcePrefix = _source.Name.ToLowerInvariant().Replace(" ", "_");
            var outFile = Path.Combine(
                _outputPath, $"{sourcePrefix}_corpus_{DateTime.UtcNow:yyyyMMdd_HHmmss}.ndjson");

            await using var writer = new StreamWriter(outFile, append: false, encoding: Encoding.UTF8);

            foreach (var id in ids)
            {
                await semaphore.WaitAsync(ct);

                tasks.Add(Task.Run(async () =>
                {
                    try
                    {
                        var record = await FetchRecordWithRetryAsync(id, startYear, endYear, ct);

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

                        // Respect source-specific rate limit
                        await Task.Delay(_source.RateLimitMs, ct);
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
        // Fetch a single record with retry logic and year filtering
        // -----------------------------------------------------------------
        private async Task<AestheticRecord?> FetchRecordWithRetryAsync(
            string id,
            int? startYear,
            int? endYear,
            CancellationToken ct)
        {
            for (var attempt = 0; attempt < RetryCount; attempt++)
            {
                try
                {
                    var record = await _source.FetchRecordAsync(id, startYear, endYear, ct);

                    if (record is null)
                        return null;

                    // Apply additional year filtering (source may have already filtered)
                    if (startYear.HasValue && record.YearCreated < startYear.Value)
                        return null;
                    if (endYear.HasValue && record.YearCreated > endYear.Value)
                        return null;

                    // Enrich with computed fields
                    record = record with
                    {
                        MerkleHash = ComputeMerkleHash(record.SourceId, record.Title, record.YearCreated?.ToString() ?? ""),
                        IngestedAt = DateTime.UtcNow
                    };

                    return record;
                }
                catch (HttpRequestException ex)
                    when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
                {
                    return null; // object no longer exists, do not retry
                }
                catch (HttpRequestException ex)
                    when (ex.StatusCode == System.Net.HttpStatusCode.TooManyRequests ||
                          ex.StatusCode == System.Net.HttpStatusCode.ServiceUnavailable ||
                          ex.StatusCode == System.Net.HttpStatusCode.Forbidden)
                {
                    var wait = (ex.StatusCode == System.Net.HttpStatusCode.Forbidden)
                        ? 15000 * (attempt + 1)
                        : 10000 * (attempt + 1);

                    Console.WriteLine($"[THROTTLE] {_source.Name} id {id} — HTTP {(int?)ex.StatusCode}, backing off {wait}ms (attempt {attempt + 1}/{RetryCount})");

                    if (attempt < RetryCount - 1)
                        await Task.Delay(wait, ct);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[DEBUG] {_source.Name} id {id} error: {ex.GetType().Name}: {ex.Message}");
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
        private static string MapDomain(string department)
        {
            switch (department)
            {
                case var _ when department.Contains("Architecture", StringComparison.OrdinalIgnoreCase):
                    return "Architecture";
                case var _ when department.Contains("Drawings", StringComparison.OrdinalIgnoreCase):
                case var _ when department.Contains("Paintings", StringComparison.OrdinalIgnoreCase):
                case var _ when department.Contains("Prints", StringComparison.OrdinalIgnoreCase):
                case var _ when department.Contains("Sculpture", StringComparison.OrdinalIgnoreCase):
                    return "Art";
                case var _ when department.Contains("Musical", StringComparison.OrdinalIgnoreCase):
                    return "Music";
                default:
                    return "Art";
            }
        }

        private void Report(int current, int total, string message) =>
            _progress?.Report(new IngestProgress(current, total, message));
    }
}