namespace SeldonEngine.Audit;

// -------------------------------------------------------------------------
// AuditReport — updated from your v3 version
// Added: CrossCultural field for Western vs non-Western comparison
// Your existing fields preserved exactly as-is
// -------------------------------------------------------------------------

public class AuditReport
{
    public int                                       TotalRecords            { get; set; }
    public Dictionary<int, DecadeBucket>             DecadeDistribution      { get; set; } = new();
    public Dictionary<int, DecadeBucket>             AcquisitionDistribution { get; set; } = new();
    public Dictionary<string, SacredSecularBucket>  SacredSecularByEra      { get; set; } = new();
    public Dictionary<string, int>                  CulturalBreakdown       { get; set; } = new();
    public Dictionary<string, int>                  DomainBreakdown         { get; set; } = new();
    public List<KondratievAlignmentBucket>           KondratievAlignment     { get; set; } = [];
    public List<AnomalyRecord>                       Anomalies               { get; set; } = [];
    public List<string>                              SocraticFindings        { get; set; } = [];

    // Added in v4 — cross-cultural falsification layer
    // Populated only when non-Western records are present in corpus
    public CrossCulturalComparison?                  CrossCultural           { get; set; }
}