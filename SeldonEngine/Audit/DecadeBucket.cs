namespace SeldonEngine.Audit;

public class DecadeBucket(int decade)
{
    public int Decade         { get; } = decade;
    public int Count          { get; set; }
    public int SacredCount    { get; set; }
    public int HighlightCount { get; set; }
}