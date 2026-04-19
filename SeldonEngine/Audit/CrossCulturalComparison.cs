namespace SeldonEngine.Audit;

// -------------------------------------------------------------------------
// CrossCulturalComparison
//
// Socratic probe: do non-Western cultures show the same decade density
// as Western cultures? This is the key falsification test for whether
// the Kondratiev aesthetic signal is universal or Western-specific.
//
// Convergence (r > 0.7) = universal law candidate
// Divergence  (r < 0.3) = Western-specific signal — cannot generalise
// -------------------------------------------------------------------------

public record CrossCulturalComparison(
    int                     WesternCount,
    int                     NonWesternCount,
    Dictionary<int, int>    WesternByDecade,
    Dictionary<int, int>    NonWesternByDecade,
    Dictionary<string, int> NonWesternCultures,
    double                  PearsonCorrelation,
    string                  SocraticVerdict
);

public static class PearsonHelper
{
    public static double Compute(List<double> x, List<double> y)
    {
        if (x.Count != y.Count || x.Count < 2) return 0;
        var xMean = x.Average();
        var yMean = y.Average();
        var num   = x.Zip(y, (a, b) => (a - xMean) * (b - yMean)).Sum();
        var denX  = Math.Sqrt(x.Sum(a => Math.Pow(a - xMean, 2)));
        var denY  = Math.Sqrt(y.Sum(b => Math.Pow(b - yMean, 2)));
        return denX * denY > 0 ? num / (denX * denY) : 0;
    }

    public static string Verdict(double r) => r switch
    {
        > 0.7  => $"[SIGNAL]     r={r:F3} Strong correlation — peaks align across cultures. Universal signal candidate.",
        > 0.4  => $"[WEAK]       r={r:F3} Moderate correlation — partial alignment. Requires deeper analysis.",
        > 0.0  => $"[INCONCLUSIVE] r={r:F3} Weak positive — Western and non-Western loosely aligned.",
        _      => $"[ANOMALY]    r={r:F3} Negative/zero correlation — peaks diverge. Culture-specific signal."
    };
}