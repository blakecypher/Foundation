using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace SeldonEngine.Corpus;

public class MetSource : ICorpusSource
{
    private readonly HttpClient _http;
    private readonly int? _departmentId;
    private readonly bool _hasImages;

    public string Name => "MetMuseum";
    public int RateLimitMs => 250; // Met requests 80 requests/second max

    public MetSource(int? departmentId = null, bool hasImages = true)
    {
        _http = new HttpClient { Timeout = TimeSpan.FromSeconds(30) };
        _http.DefaultRequestHeaders.Add("User-Agent", "SeldonEngine/1.0");
        _departmentId = departmentId;
        _hasImages = hasImages;
    }

    public async Task<List<string>> FetchObjectIdsAsync(
        int? startYear, int? endYear, CancellationToken ct)
    {
        var ids = new List<string>();
        var url = "https://collectionapi.metmuseum.org/public/collection/v1/objects";

        var queryParams = new List<string>();
        if (_departmentId.HasValue)
            queryParams.Add($"departmentIds={_departmentId.Value}");
        if (_hasImages)
            queryParams.Add("isOnView=true"); // Proxy for images + metadata quality

        if (queryParams.Any())
            url += "?" + string.Join("&", queryParams);

        var response = await _http.GetStringAsync(url, ct);
        var envelope = JsonSerializer.Deserialize<ObjectListEnvelope>(response);

        if (envelope?.ObjectIDs != null)
            ids.AddRange(envelope.ObjectIDs.Select(id => id.ToString()));

        await Task.Delay(RateLimitMs, ct);
        return ids;
    }

    public async Task<AestheticRecord?> FetchRecordAsync(
        string id, int? startYear, int? endYear, CancellationToken ct)
    {
        var url = $"https://collectionapi.metmuseum.org/public/collection/v1/objects/{id}";
        var json = await _http.GetStringAsync(url, ct);
        var obj = JsonSerializer.Deserialize<MetObject>(json);

        if (obj is null) return null;

        // Skip if no image and we require images
        if (_hasImages && string.IsNullOrEmpty(obj.PrimaryImage))
            return null;

        // Parse year
        var year = ParseYear(obj.ObjectDate, obj.ObjectBeginDate, obj.ObjectEndDate);
        if (year is null) return null;

        // Apply year filter
        if (startYear.HasValue && year < startYear.Value)
            return null;
        if (endYear.HasValue && year > endYear.Value)
            return null;

        var domain = MapDomain(obj.Department ?? "");
        var isReligious = ClassifyReligious(obj);

        return new AestheticRecord
        {
            MerkleHash = ComputeHash(id, obj.Title ?? "", year.ToString()),
            SourceId = id,
            Source = "MetMuseum",
            YearCreated = year,
            YearAcquired = int.TryParse(obj.AccessionYear, out var ay) ? ay : null,
            Title = obj.Title ?? string.Empty,
            Country = obj.Country ?? string.Empty,
            Culture = obj.Culture ?? string.Empty,
            Region = obj.Region ?? string.Empty,
            Period = obj.Period ?? string.Empty,
            Dynasty = obj.Dynasty ?? string.Empty,
            Domain = domain,
            Medium = obj.Medium ?? string.Empty,
            Classification = obj.Classification ?? string.Empty,
            Department = obj.Department ?? string.Empty,
            Dimensions = obj.Dimensions ?? string.Empty,
            HasImage = !string.IsNullOrEmpty(obj.PrimaryImage),
            IsHighlight = obj.IsHighlight,
            IsReligious = isReligious,
            ArtistName = obj.ArtistDisplayName ?? string.Empty,
            ArtistNationality = obj.ArtistNationality ?? string.Empty,
            ArtistBeginDate = obj.ArtistBeginDate ?? string.Empty,
            RawJson = json,
            IngestedAt = DateTime.UtcNow
        };
    }

    // -----------------------------------------------------------------
    // Year parsing — Met dates are messy strings
    // e.g. "ca. 1750", "1750–60", "18th century", "before 1800"
    // -----------------------------------------------------------------
    private static int? ParseYear(string? dateStr, int? beginDate = null, int? endDate = null)
    {
        if (beginDate.HasValue && endDate.HasValue)
            return (beginDate.Value + endDate.Value) / 2;

        if (beginDate.HasValue) return beginDate.Value;
        if (endDate.HasValue) return endDate.Value;

        if (string.IsNullOrWhiteSpace(dateStr)) return null;

        // Strip common prefixes
        dateStr = dateStr
            .Replace("ca.", "").Replace("c.", "").Replace("circa", "")
            .Replace("before", "").Replace("after", "").Replace("probably", "")
            .Trim();

        // Range: take midpoint
        if (dateStr.Contains('–') || dateStr.Contains('-'))
        {
            var sep = dateStr.Contains('–') ? '–' : '-';
            var parts = dateStr.Split(sep, 2);
            if (int.TryParse(parts[0].Trim(), out var y1) &&
                int.TryParse(parts[1].Trim(), out var y2))
                return (y1 + y2) / 2;
        }

        // Direct parse
        if (int.TryParse(dateStr.Trim(), out var year) && year > 0 && year < 2100)
            return year;

        // Century approximation
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
    // Domain mapping — Art / Architecture / Music / Other
    // -----------------------------------------------------------------
    private static string MapDomain(string department) => department switch
    {
        var d when d.Contains("Architecture", StringComparison.OrdinalIgnoreCase) => "Architecture",
        var d when d.Contains("Drawings", StringComparison.OrdinalIgnoreCase) => "Art",
        var d when d.Contains("Paintings", StringComparison.OrdinalIgnoreCase) => "Art",
        var d when d.Contains("Prints", StringComparison.OrdinalIgnoreCase) => "Art",
        var d when d.Contains("Sculpture", StringComparison.OrdinalIgnoreCase) => "Art",
        var d when d.Contains("Musical", StringComparison.OrdinalIgnoreCase) => "Music",
        _ => "Art"
    };

    // -----------------------------------------------------------------
    // Sacred/secular classification
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

    private static string ComputeHash(params string?[] parts) =>
        Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(string.Join("|", parts)))).ToLower();

    private static readonly HashSet<string> SacredMarkers = new(StringComparer.OrdinalIgnoreCase)
    {
        "religious", "sacred", "church", "cathedral", "temple", "mosque",
        "altar", "icon", "devotional", "liturgical", "bible", "torah",
        "quran", "saint", "madonna", "christ", "buddha", "deity", "ritual",
        "votive", "ex-voto", "reliquary", "chalice", "crucifix", "triptych"
    };
}
