namespace SeldonEngine.Audit;

public record AnomalyRecord(
    int    Decade,
    int    RecordCount,
    double ZScore,
    string Direction,
    string KondratievPhase,
    string SocraticNote
);