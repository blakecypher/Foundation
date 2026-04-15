namespace SeldonEngine.Audit;

public class AuditReport
{
    public int                                        TotalRecords       { get; set; }
    public Dictionary<int, DecadeBucket>              DecadeDistribution { get; set; } = new();
    public Dictionary<string, SacredSecularBucket>   SacredSecularByEra { get; set; } = new();
    public Dictionary<string, int>                   CulturalBreakdown  { get; set; } = new();
    public Dictionary<string, int>                   DomainBreakdown    { get; set; } = new();
    public List<KondratievAlignmentBucket>            KondratievAlignment { get; set; } = [];
    public List<AnomalyRecord>                        Anomalies          { get; set; } = [];
    public List<string>                               SocraticFindings   { get; set; } = [];
}