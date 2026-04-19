namespace SeldonEngine.Corpus;

public interface ICorpusSource
{
    string Name { get; }
    Task<List<string>> FetchObjectIdsAsync(int? startYear, int? endYear, CancellationToken ct);
    Task<AestheticRecord?> FetchRecordAsync(string id, int? startYear, int? endYear, CancellationToken ct);
    int RateLimitMs { get; }
}