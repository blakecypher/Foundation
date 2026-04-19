using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Web;

namespace SeldonEngine.Corpus;

public class EuropeanaSource : ICorpusSource
{
    private readonly HttpClient _http;
    private readonly string _apiKey;
    private readonly string _query;

    public string Name => "Europeana";
    public int RateLimitMs => 500; // Europeana is more tolerant than Met

    // Register at https://pro.europeana.eu/page/get-api-tiffernalk
    private const string ApiKeyEnvVar = "EUROPEANA_API_KEY";

    public EuropeanaSource(string query)
    {
        _http = new HttpClient { Timeout = TimeSpan.FromSeconds(30) };
        _http.DefaultRequestHeaders.Add("User-Agent", "SeldonEngine/1.0");
        _apiKey = Environment.GetEnvironmentVariable(ApiKeyEnvVar) 
            ?? throw new InvalidOperationException($"Missing {ApiKeyEnvVar}");
        _query = query;
    }

    public async Task<List<string>> FetchObjectIdsAsync(
        int? startYear, int? endYear, CancellationToken ct)
    {
        var ids = new List<string>();
        var cursor = "*";
        var rows = 100;

        while (!string.IsNullOrEmpty(cursor))
        {
            var url = BuildSearchUrl(cursor, rows, startYear, endYear);
            var response = await _http.GetStringAsync(url, ct);
            var doc = JsonDocument.Parse(response);

            var items = doc.RootElement.GetProperty("items");
            foreach (var item in items.EnumerateArray())
                ids.Add(item.GetProperty("id").GetString()!);

            cursor = doc.RootElement.TryGetProperty("nextCursor", out var next) 
                ? next.GetString() : null;

            await Task.Delay(RateLimitMs, ct);
        }

        return ids;
    }

    public async Task<AestheticRecord?> FetchRecordAsync(
        string id, int? startYear, int? endYear, CancellationToken ct)
    {
        var url = $"https://api.europeana.eu/record/v2/{id}.json?wskey={_apiKey}&profile=rich";
        var json = await _http.GetStringAsync(url, ct);
        var doc = JsonDocument.Parse(json);
        var obj = doc.RootElement.GetProperty("object");

        var year = ParseEuropeanaYear(obj, startYear, endYear);
        if (year is null) return null;

        var title = obj.TryGetProperty("title", out var t) && t.ValueKind == JsonValueKind.Array
            ? t[0].GetString() : string.Empty;

        var country = obj.TryGetProperty("country", out var c) && c.ValueKind == JsonValueKind.Array
            ? c[0].GetString() : string.Empty;

        var domain = MapEuropeanaType(obj);

        return new AestheticRecord
        {
            MerkleHash = ComputeHash(id, title ?? "", year.ToString()),
            SourceId = id,
            Source = obj.TryGetProperty("dataProvider", out var dp) && dp.ValueKind == JsonValueKind.Array
                ? dp[0].GetString() ?? "Europeana" : "Europeana",
            YearCreated = year,
            Title = title ?? string.Empty,
            Country = country ?? string.Empty,
            Culture = country ?? string.Empty,
            Domain = domain,
            HasImage = obj.TryGetProperty("edmPreview", out _),
            IsReligious = ClassifyReligious(title, obj),
            RawJson = json,
            IngestedAt = DateTime.UtcNow
        };
    }

    private string BuildSearchUrl(string cursor, int rows, int? startYear, int? endYear)
    {
        var url = $"https://api.europeana.eu/record/v2/search.json?wskey={_apiKey}";
        url += $"&query={HttpUtility.UrlEncode(_query)}";
        url += $"&rows={rows}";
        url += "&profile=rich";
        if (cursor != "*") url += $"&cursor={HttpUtility.UrlEncode(cursor)}";
        if (startYear.HasValue || endYear.HasValue)
            url += $"&qf=YEAR:[{startYear ?? 1000}+TO+{endYear ?? 2025}]";
        return url;
    }

    private int? ParseEuropeanaYear(JsonElement obj, int? startYear, int? endYear)
    {
        // Europeana year is in proxy_dcterms_created.def[0]
        if (!obj.TryGetProperty("proxies", out var proxies)) return null;
        foreach (var proxy in proxies.EnumerateArray())
        {
            if (!proxy.TryGetProperty("dctermsCreated", out var dc)) continue;
            if (!dc.TryGetProperty("def", out var def) || def.ValueKind != JsonValueKind.Array) continue;
            var yearStr = def[0].GetString();
            if (int.TryParse(yearStr, out var year) && year >= (startYear ?? 0) && year <= (endYear ?? 9999))
                return year;
        }
        return null;
    }

    private static string MapEuropeanaType(JsonElement obj)
    {
        if (!obj.TryGetProperty("type", out var type) || type.ValueKind != JsonValueKind.String)
            return "Art";
        return type.GetString() switch
        {
            "IMAGE" => "Art",
            "SOUND" => "Music",
            "TEXT" => "Architecture",
            _ => "Art"
        };
    }

    private static bool ClassifyReligious(string? title, JsonElement obj)
    {
        var text = title ?? string.Empty;
        if (obj.TryGetProperty("dcDescription", out var desc) && desc.ValueKind == JsonValueKind.Array)
            text += " " + string.Join(" ", desc.EnumerateArray().Select(d => d.GetString()));
        return SacredMarkers.Any(m => text.Contains(m, StringComparison.OrdinalIgnoreCase));
    }

    private static string ComputeHash(params string?[] parts) =>
        Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(string.Join("|", parts)))).ToLower();

    private static readonly HashSet<string> SacredMarkers = new(StringComparer.OrdinalIgnoreCase)
    {
        "temple", "buddha", "bodhisattva", "shrine", "deity", "hindu", "islamic", "mosque",
        "ritual", "ceremonial", "ancestor", "spirit"
    };
}