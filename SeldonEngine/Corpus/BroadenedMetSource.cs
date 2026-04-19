using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace SeldonEngine.Corpus;

// -------------------------------------------------------------------------
// SELDON ENGINE — BroadenedMetSource : ICorpusSource
//
// Replaces BroadenedIngest.cs — now plugs into ICorpusSource architecture.
//
// Socratic mandate from Step 1 audit:
//   [BLOCKED]  82% Western — needs Asian, Islamic, Medieval
//   [MISSING]  Music domain — 0 records in first run
//   [HELD]     1930s anomaly — needs broader context
//
// Department targets in priority order:
//   17  Medieval Art          sacred baseline, pre-industrial signal
//   18  Musical Instruments   first music domain substrate
//   6   Asian Art             largest non-Western Met collection
//   14  Islamic Art           non-Western sacred signal
//   13  Greek and Roman Art   pre-industrial civilizational baseline
//   3   Ancient Near Eastern  deepest historical baseline
// -------------------------------------------------------------------------

public record DepartmentTarget(
    int    Id,
    string Name,
    string DefaultDomain,
    int    Priority,
    string SocraticNote,
    bool   IncludePreIndustrial = false  // Include BCE / pre-1400 dates
);

public static class BroadenedMetConfig
{
    public static readonly List<DepartmentTarget> Targets = new()
    {
        new(17, "Medieval Art",            "Art",   1,
            "Sacred baseline — highest religious density expected. " +
            "Will reveal whether sacred ratio actually peaks here as hypothesised.",
            IncludePreIndustrial: false),

        new(18, "Musical Instruments",     "Music", 2,
            "First music domain records. Instrument construction dates proxy " +
            "for cultural investment in sound. " +
            "Test: do instrument peaks align with visual art peaks?",
            IncludePreIndustrial: false),

        new(6,  "Asian Art",               "Art",   3,
            "Largest non-Western Met collection. Reduces Western bias from 82%. " +
            "Key falsification: does Asian production show same decade distribution as Western? " +
            "Convergence = universal law. Divergence = Western-specific signal.",
            IncludePreIndustrial: false),

        new(14, "Islamic Art",             "Art",   4,
            "Non-Western sacred signal. Islamic economic cycles independent test case. " +
            "Test: do Islamic sacred peaks align with Christian peaks or are they phase-shifted?",
            IncludePreIndustrial: false),

        new(13, "Greek and Roman Art",     "Art",   5,
            "Pre-industrial civilizational baseline. Roman economic cycles well-documented. " +
            "Test: do Kondratiev-like patterns exist before industrialization? " +
            "If yes: conservation law is deeper than capitalism.",
            IncludePreIndustrial: true),

        new(3,  "Ancient Near Eastern Art","Art",   6,
            "Deepest historical baseline. Mesopotamian, Assyrian, Persian cycles. " +
            "Tests whether proto-Kondratiev patterns exist in earliest complex civilizations.",
            IncludePreIndustrial: true),
    };
}

public class BroadenedMetSource : ICorpusSource
{
    private readonly HttpClient        _http;
    private readonly DepartmentTarget  _target;

    public string Name       => $"MetMuseum_Dept{_target.Id}_{_target.Name.Replace(" ", "")}";
    public int    RateLimitMs => 200;

    private const string BaseUrl = "https://collectionapi.metmuseum.org/public/collection/v1";

    // Extended sacred markers — covers all traditions in broadened corpus
    private static readonly HashSet<string> SacredMarkers = new(StringComparer.OrdinalIgnoreCase)
    {
        // Christian
        "religious","sacred","church","cathedral","chapel","altar","icon",
        "devotional","liturgical","bible","saint","madonna","christ","crucifix",
        "triptych","reliquary","chalice","votive","ex-voto","manuscript illuminat",
        // Islamic
        "mosque","quran","qur'an","mihrab","minbar","calligraphy","madrasa",
        "pilgrimage","hajj",
        // Buddhist / Hindu / Asian
        "buddha","buddhist","bodhisattva","temple","shrine","deity","ritual",
        "votive","offering","mandala","stupa","pagoda","shinto","vishnu",
        "shiva","ganesh","hinduism",
        // Ancient / funerary
        "cult","oracle","funerary","tomb","burial","sarcophagus","mummy",
        "canopic","ushabti",
        // General
        "prayer","worship","divine","holy","spiritual"
    };

    // Architecture markers for domain classification
    private static readonly HashSet<string> ArchMarkers = new(StringComparer.OrdinalIgnoreCase)
    {
        "architectural","architecture","building","facade","column","capital",
        "frieze","pediment","arch","vault","dome","floor plan","elevation",
        "section drawing","architectural model","architectural fragment"
    };

    public BroadenedMetSource(DepartmentTarget target)
    {
        _target = target;
        _http   = new HttpClient { Timeout = TimeSpan.FromSeconds(30) };
        _http.DefaultRequestHeaders.Add("User-Agent", "SeldonEngine/1.0");
    }

    // Factory — ingest all broadened targets in sequence
    public static IEnumerable<BroadenedMetSource> AllTargets() =>
        BroadenedMetConfig.Targets
            .OrderBy(t => t.Priority)
            .Select(t => new BroadenedMetSource(t));

    public async Task<List<string>> FetchObjectIdsAsync(
        int? startYear, int? endYear, CancellationToken ct)
    {
        // Pre-industrial departments: no date filter — include full historical range
        var dateFilter = _target.IncludePreIndustrial
            ? ""
            : $"&dateBegin={startYear ?? 1400}&dateEnd={endYear ?? 2025}";

        var url = $"{BaseUrl}/objects?departmentIds={_target.Id}&hasImages=true{dateFilter}";

        var response = await _http.GetStringAsync(url, ct);
        var envelope = JsonSerializer.Deserialize<ObjectListEnvelope>(response);
        return envelope?.ObjectIDs?.Select(id => id.ToString()).ToList() ?? new();
    }

    public async Task<AestheticRecord?> FetchRecordAsync(
        string id, int? startYear, int? endYear, CancellationToken ct)
    {
        var json = await _http.GetStringAsync($"{BaseUrl}/objects/{id}", ct);
        var obj  = JsonSerializer.Deserialize<MetObject>(json);
        if (obj is null) return null;

        var year = ParseYear(obj.ObjectDate, obj.ObjectBeginDate, obj.ObjectEndDate);
        if (year is null) return null;

        return new AestheticRecord
        {
            MerkleHash          = ComputeHash(id, obj.Title ?? "", year.ToString()),
            SourceId            = id,
            Source              = Name,
            YearCreated         = year,
            YearAcquired        = int.TryParse(obj.AccessionYear, out var ay) ? ay : null,
            Title               = obj.Title       ?? string.Empty,
            Culture             = obj.Culture     ?? string.Empty,
            Country             = obj.Country     ?? string.Empty,
            Region              = obj.Region      ?? string.Empty,
            Period              = obj.Period       ?? string.Empty,
            Dynasty             = obj.Dynasty     ?? string.Empty,
            Domain              = ClassifyDomain(obj),
            Medium              = obj.Medium      ?? string.Empty,
            Classification      = obj.Classification ?? string.Empty,
            Department          = obj.Department  ?? string.Empty,
            Dimensions          = obj.Dimensions  ?? string.Empty,
            HasImage            = !string.IsNullOrEmpty(obj.PrimaryImage),
            IsHighlight         = obj.IsHighlight,
            IsReligious         = ClassifyReligious(obj),
            ArtistName          = obj.ArtistDisplayName ?? string.Empty,
            ArtistNationality   = obj.ArtistNationality ?? string.Empty,
            ArtistBeginDate     = obj.ArtistBeginDate   ?? string.Empty,
            RawJson             = json,
            IngestedAt          = DateTime.UtcNow
        };
    }

    // -----------------------------------------------------------------
    // Domain classification — extended for broadened corpus
    // -----------------------------------------------------------------
    private string ClassifyDomain(MetObject obj)
    {
        if (_target.DefaultDomain == "Music") return "Music";

        var cls = obj.Classification ?? string.Empty;
        var med = obj.Medium         ?? string.Empty;

        foreach (var m in ArchMarkers)
            if (cls.Contains(m, StringComparison.OrdinalIgnoreCase) ||
                med.Contains(m, StringComparison.OrdinalIgnoreCase))
                return "Architecture";

        return "Art";
    }

    // -----------------------------------------------------------------
    // Sacred classification — extended for all traditions
    // -----------------------------------------------------------------
    private static bool ClassifyReligious(MetObject obj)
    {
        var fields = new[]
        {
            obj.Title, obj.Classification, obj.Department,
            obj.Culture, obj.Medium, obj.Period, obj.ObjectName
        };

        foreach (var f in fields)
        {
            if (string.IsNullOrEmpty(f)) continue;
            if (SacredMarkers.Any(m => f.Contains(m, StringComparison.OrdinalIgnoreCase)))
                return true;
        }

        // Tag-based — richer signal
        if (obj.Tags is not null)
            foreach (var tag in obj.Tags)
                if (!string.IsNullOrEmpty(tag.Term) &&
                    SacredMarkers.Any(m => tag.Term.Contains(m, StringComparison.OrdinalIgnoreCase)))
                    return true;

        return false;
    }

    // -----------------------------------------------------------------
    // Year parsing — handles BCE dates for classical / ancient depts
    // -----------------------------------------------------------------
    private static int? ParseYear(string? dateStr, int? begin = null, int? end = null)
    {
        if (begin.HasValue && end.HasValue) return (begin.Value + end.Value) / 2;
        if (begin.HasValue) return begin.Value;
        if (end.HasValue)   return end.Value;
        if (string.IsNullOrWhiteSpace(dateStr)) return null;

        bool isBce = dateStr.Contains("B.C", StringComparison.OrdinalIgnoreCase) ||
                     dateStr.Contains("BCE", StringComparison.OrdinalIgnoreCase);

        var s = dateStr
            .Replace("ca.", "").Replace("c.", "").Replace("circa", "")
            .Replace("before","").Replace("after","").Replace("probably","")
            .Replace("B.C.E.","").Replace("B.C.","").Replace("BCE","")
            .Replace("A.D.","").Replace("CE","")
            .Replace("n.d.","").Trim();

        // Range — take midpoint
        var sep = s.Contains('–') ? '–' : '-';
        if (s.Contains(sep))
        {
            var parts = s.Split(sep, 2);
            if (int.TryParse(parts[0].Trim(), out var y1) &&
                int.TryParse(parts[1].Trim(), out var y2))
                return isBce ? -((y1 + y2) / 2) : (y1 + y2) / 2;
        }

        if (int.TryParse(s.Trim(), out var yr))
            return isBce ? -yr : yr;

        // Century approximation
        if (s.Contains("century", StringComparison.OrdinalIgnoreCase))
        {
            for (int c = 1; c <= 21; c++)
                foreach (var ord in new[] { $"{c}th", $"{c}st", $"{c}nd", $"{c}rd" })
                    if (s.Contains(ord, StringComparison.OrdinalIgnoreCase))
                    {
                        var mid = (c - 1) * 100 + 50;
                        return isBce ? -mid : mid;
                    }
        }

        return null;
    }

    private static string ComputeHash(params string?[] parts) =>
        Convert.ToHexString(
            SHA256.HashData(Encoding.UTF8.GetBytes(string.Join("|", parts)))
        ).ToLower();
}