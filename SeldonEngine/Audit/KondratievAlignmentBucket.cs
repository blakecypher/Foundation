namespace SeldonEngine.Audit;

public record KondratievAlignmentBucket(
    string Phase,
    int    Start,
    int    End,
    int    RecordCount,
    double RecordsPerYear,
    double SacredRatio
);