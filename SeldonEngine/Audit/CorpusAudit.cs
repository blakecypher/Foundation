using System.Text;
using System.Text.Json;
using SeldonEngine.Corpus;

namespace SeldonEngine.Audit
{
    // -------------------------------------------------------------------------
    // SELDON ENGINE — Corpus Audit
    // Socratic Layer: Step 1 interrogation of the Met corpus
    //
    // This audit does not assume the Kondratiev hypothesis is correct.
    // It asks the data what it shows — then asks whether that is meaningful.
    //
    // Four interrogations:
    //   1. Decade distribution      — does production density track Kondratiev summers?
    //   2. Sacred / secular ratio   — does the leading indicator trend visibly?
    //   3. Cultural breadth         — is this corpus too Western to be universal?
    //   4. Anomaly surface          — what does the data show that the model does not predict?
    // -------------------------------------------------------------------------

    public class CorpusAudit(string corpusPath)
    {
        // Known Kondratiev wave phases for comparison
        // Socratic probe: are these dates themselves contested?
        // Answer: yes — treat as approximate ±5yr, not ground truth
        private static readonly List<KondratievPhase> KnownPhases =
        [
            new KondratievPhase("Wave 1 Spring", 1787, 1800),
            new KondratievPhase("Wave 1 Summer", 1800, 1815),
            new KondratievPhase("Wave 1 Autumn", 1815, 1825),
            new KondratievPhase("Wave 1 Winter", 1825, 1845),
            new KondratievPhase("Wave 2 Spring", 1845, 1858),
            new KondratievPhase("Wave 2 Summer", 1858, 1873),
            new KondratievPhase("Wave 2 Autumn", 1873, 1883),
            new KondratievPhase("Wave 2 Winter", 1883, 1897),
            new KondratievPhase("Wave 3 Spring", 1897, 1907),
            new KondratievPhase("Wave 3 Summer", 1907, 1920),
            new KondratievPhase("Wave 3 Autumn", 1920, 1929),
            new KondratievPhase("Wave 3 Winter", 1929, 1945),
            new KondratievPhase("Wave 4 Spring", 1945, 1958),
            new KondratievPhase("Wave 4 Summer", 1958, 1973),
            new KondratievPhase("Wave 4 Autumn", 1973, 1983),
            new KondratievPhase("Wave 4 Winter", 1983, 1997),
            new KondratievPhase("Wave 5 Spring", 1997, 2007),
            new KondratievPhase("Wave 5 Summer", 2007, 2015),
            new KondratievPhase("Wave 5 Autumn", 2015, 2020),
            new KondratievPhase("Wave 5 Winter", 2020, 2030)
        ];
        private static readonly string[] SourceArray = ["American","British","French","Italian","German","Dutch / Flemish"
        ];

        public async Task<AuditReport> RunAsync()
        {
            Console.WriteLine("SELDON ENGINE — Corpus Audit");
            Console.WriteLine("============================");
            Console.WriteLine("Loading corpus...");

            var records = await LoadCorpusAsync();

            Console.WriteLine($"Loaded {records.Count:N0} records.");
            Console.WriteLine();

            var report = new AuditReport
            {
                TotalRecords      = records.Count,
                DecadeDistribution = BuildDecadeDistribution(records),
                SacredSecularByEra = BuildSacredSecularRatio(records),
                CulturalBreakdown  = BuildCulturalBreakdown(records),
                DomainBreakdown    = BuildDomainBreakdown(records),
                KondratievAlignment = BuildKondratievAlignment(records),
                Anomalies          = SurfaceAnomalies(records),
                SocraticFindings   = []
            };

            // Socratic interrogation — automated first pass
            InterrogateReport(report);

            return report;
        }

        // -----------------------------------------------------------------
        // 1. Decade distribution
        // Hypothesis: more art is produced / acquired in Kondratiev summers
        // Falsification target: flat distribution would challenge the hypothesis
        // -----------------------------------------------------------------
        private static Dictionary<int, DecadeBucket> BuildDecadeDistribution(
            List<AestheticRecord> records)
        {
            var buckets = new Dictionary<int, DecadeBucket>();

            foreach (var r in records.Where(r => r.YearCreated.HasValue))
            {
                var decade = (r.YearCreated!.Value / 10) * 10;
                if (!buckets.ContainsKey(decade))
                    buckets[decade] = new DecadeBucket(decade);

                buckets[decade].Count++;
                if (r.IsReligious) buckets[decade].SacredCount++;
                if (r.IsHighlight) buckets[decade].HighlightCount++;
            }

            return buckets.OrderBy(k => k.Key)
                          .ToDictionary(k => k.Key, v => v.Value);
        }

        // -----------------------------------------------------------------
        // 2. Sacred / secular ratio by era
        // Hypothesis: sacred building/art rate declines in Kondratiev winters
        // and collapses in the terminal phase — leading the institutional decline
        // -----------------------------------------------------------------
        private static Dictionary<string, SacredSecularBucket> BuildSacredSecularRatio(
            List<AestheticRecord> records)
        {
            var eras = new Dictionary<string, SacredSecularBucket>
            {
                ["Pre-industrial (before 1780)"] = new(),
                ["Wave 1-2 (1780-1850)"]         = new(),
                ["Wave 2-3 (1850-1920)"]         = new(),
                ["Wave 3-4 (1920-1970)"]         = new(),
                ["Wave 4-5 (1970-2010)"]         = new(),
                ["Wave 5-6 (2010-present)"]      = new(),
            };

            foreach (var r in records.Where(r => r.YearCreated.HasValue))
            {
                var y   = r.YearCreated!.Value;
                var era = y switch
                {
                    < 1780 => "Pre-industrial (before 1780)",
                    < 1850 => "Wave 1-2 (1780-1850)",
                    < 1920 => "Wave 2-3 (1850-1920)",
                    < 1970 => "Wave 3-4 (1920-1970)",
                    < 2010 => "Wave 4-5 (1970-2010)",
                    _      => "Wave 5-6 (2010-present)"
                };

                eras[era].Total++;
                if (r.IsReligious) eras[era].Sacred++;
                else               eras[era].Secular++;
            }

            return eras;
        }

        // -----------------------------------------------------------------
        // 3. Cultural breadth
        // Socratic probe: is a Western-dominated corpus valid for
        // claims about universal civilizational conservation laws?
        // -----------------------------------------------------------------
        private static Dictionary<string, int> BuildCulturalBreakdown(
            List<AestheticRecord> records)
        {
            return records
                .Where(r => !string.IsNullOrEmpty(r.Culture))
                .GroupBy(r => NormaliseCulture(r.Culture))
                .OrderByDescending(g => g.Count())
                .Take(30)
                .ToDictionary(g => g.Key, g => g.Count());
        }

        private static string NormaliseCulture(string culture)
        {
            if (string.IsNullOrEmpty(culture)) return "Unknown";
            // Broad groupings for civilizational analysis
            if (culture.Contains("American",  StringComparison.OrdinalIgnoreCase)) return "American";
            if (culture.Contains("British",   StringComparison.OrdinalIgnoreCase)) return "British";
            if (culture.Contains("French",    StringComparison.OrdinalIgnoreCase)) return "French";
            if (culture.Contains("Italian",   StringComparison.OrdinalIgnoreCase)) return "Italian";
            if (culture.Contains("German",    StringComparison.OrdinalIgnoreCase)) return "German";
            if (culture.Contains("Dutch",     StringComparison.OrdinalIgnoreCase)) return "Dutch / Flemish";
            if (culture.Contains("Flemish",   StringComparison.OrdinalIgnoreCase)) return "Dutch / Flemish";
            if (culture.Contains("Chinese",   StringComparison.OrdinalIgnoreCase)) return "Chinese";
            if (culture.Contains("Japanese",  StringComparison.OrdinalIgnoreCase)) return "Japanese";
            if (culture.Contains("Islamic",   StringComparison.OrdinalIgnoreCase)) return "Islamic";
            if (culture.Contains("Egyptian",  StringComparison.OrdinalIgnoreCase)) return "Egyptian";
            if (culture.Contains("Greek",     StringComparison.OrdinalIgnoreCase)) return "Greek";
            if (culture.Contains("Roman",     StringComparison.OrdinalIgnoreCase)) return "Roman";
            if (culture.Contains("Indian",    StringComparison.OrdinalIgnoreCase)) return "Indian";
            return culture.Length > 30 ? culture[..30] : culture;
        }

        // -----------------------------------------------------------------
        // 4. Domain breakdown
        // -----------------------------------------------------------------
        private static Dictionary<string, int> BuildDomainBreakdown(
            List<AestheticRecord> records) =>
            records
                .GroupBy(r => r.Domain)
                .ToDictionary(g => g.Key, g => g.Count());

        // -----------------------------------------------------------------
        // 5. Kondratiev phase alignment
        // The key question: does record density per phase correlate with
        // summer (expansion) phases having more production?
        // -----------------------------------------------------------------
        private static List<KondratievAlignmentBucket> BuildKondratievAlignment(
            List<AestheticRecord> records)
        {
            var buckets = new List<KondratievAlignmentBucket>();

            foreach (var phase in KnownPhases)
            {
                var phaseRecords = records
                    .Where(r => r.YearCreated.HasValue &&
                                r.YearCreated.Value >= phase.Start &&
                                r.YearCreated.Value <  phase.End)
                    .ToList();

                var yearsInPhase  = phase.End - phase.Start;
                var countPerYear  = yearsInPhase > 0
                    ? (double)phaseRecords.Count / yearsInPhase
                    : 0;

                var sacredRatio   = phaseRecords.Count > 0
                    ? (double)phaseRecords.Count(r => r.IsReligious) / phaseRecords.Count
                    : 0;

                buckets.Add(new KondratievAlignmentBucket(
                    Phase:        phase.Name,
                    Start:        phase.Start,
                    End:          phase.End,
                    RecordCount:  phaseRecords.Count,
                    RecordsPerYear: countPerYear,
                    SacredRatio:  sacredRatio
                ));
            }

            return buckets;
        }

        // -----------------------------------------------------------------
        // 6. Anomaly surface
        // Socratic imperative: what does the data show that the model
        // does NOT predict? These are the most valuable findings.
        // -----------------------------------------------------------------
        private static List<AnomalyRecord> SurfaceAnomalies(
            List<AestheticRecord> records)
        {
            var anomalies = new List<AnomalyRecord>();

            var byDecade = records
                .Where(r => r.YearCreated.HasValue)
                .GroupBy(r => (r.YearCreated!.Value / 10) * 10)
                .ToDictionary(g => g.Key, g => g.ToList());

            if (byDecade.Count < 3) return anomalies;

            var counts       = byDecade.Values.Select(v => (double)v.Count).ToList();
            var mean         = counts.Average();
            var stdDev       = Math.Sqrt(counts.Select(c => Math.Pow(c - mean, 2)).Average());

            foreach (var (decade, recs) in byDecade.OrderBy(k => k.Key))
            {
                var zscore = stdDev > 0 ? (recs.Count - mean) / stdDev : 0;

                // Flag decades more than 2 standard deviations from mean
                if (Math.Abs(zscore) > 2.0)
                {
                    var phase    = KnownPhases
                        .FirstOrDefault(p => decade >= p.Start && decade < p.End);

                    anomalies.Add(new AnomalyRecord(
                        Decade:      decade,
                        RecordCount: recs.Count,
                        ZScore:      zscore,
                        Direction:   zscore > 0 ? "Surplus" : "Deficit",
                        KondratievPhase: phase?.Name ?? "Unknown",
                        SocraticNote: zscore > 0
                            ? $"Production surplus in {decade}s — expected in Kondratiev summer?"
                            : $"Production deficit in {decade}s — suppression, loss, or winter signal?"
                    ));
                }
            }

            return anomalies.OrderByDescending(a => Math.Abs(a.ZScore)).ToList();
        }

        // -----------------------------------------------------------------
        // Socratic interrogation — automated first pass
        // These are the questions the Socratic layer asks of every pattern
        // before it is allowed to proceed toward invariant status
        // -----------------------------------------------------------------
        private static void InterrogateReport(AuditReport report)
        {
            var findings = report.SocraticFindings;

            // Interrogation 1: Is the decade distribution non-random?
            var decades = report.DecadeDistribution.Values
                .Select(b => (double)b.Count).ToList();

            if (decades.Count > 0)
            {
                var mean   = decades.Average();
                var stdDev = Math.Sqrt(decades.Select(c => Math.Pow(c - mean, 2)).Average());
                var cv     = mean > 0 ? stdDev / mean : 0;

                findings.Add(cv > 0.5
                    ? $"[SIGNAL] Decade distribution is highly uneven (CV={cv:F2}). " +
                      $"Non-random production density confirmed. Kondratiev alignment warranted."
                    : $"[CAUTION] Decade distribution is relatively flat (CV={cv:F2}). " +
                      $"Weak density variation. Socratic probe: is this corpus too curated to reflect production cycles?");
            }

            // Interrogation 2: Does sacred ratio decline over time?
            var eras     = report.SacredSecularByEra.Values.ToList();
            var ratios   = eras.Select(e => e.Total > 0 ? (double)e.Sacred / e.Total : 0).ToList();

            if (ratios.Count >= 3)
            {
                var isDecline = ratios[0] > ratios[^1];
                findings.Add(isDecline
                    ? $"[SIGNAL] Sacred ratio declines across eras ({ratios[0]:P0} → {ratios[^1]:P0}). " +
                      $"Institutional religion signal present. Consistent with Wave 6 terminal hypothesis."
                    : $"[ANOMALY] Sacred ratio does NOT decline monotonically. " +
                      $"Socratic probe: is the sacred/secular classification reliable, " +
                      $"or does this indicate genuine cultural variation?");
            }

            // Interrogation 3: Western dominance check
            var total    = report.CulturalBreakdown.Values.Sum();
            var western  = report.CulturalBreakdown
                .Where(k => SourceArray.Contains(k.Key))
                .Sum(k => k.Value);

            var westernPct = total > 0 ? (double)western / total : 0;

            findings.Add(westernPct > 0.7
                ? $"[CAUTION] {westernPct:P0} of records are Western cultural origin. " +
                  $"Cross-civilizational claims require Europeana ingest before promotion to invariant."
                : $"[SIGNAL] Cultural distribution is sufficiently broad ({westernPct:P0} Western). " +
                  $"Cross-cultural analysis viable from this corpus alone.");

            // Interrogation 4: Anomaly interpretation
            foreach (var anomaly in report.Anomalies.Take(3))
            {
                findings.Add($"[ANOMALY] {anomaly.Decade}s: {anomaly.Direction} " +
                             $"(z={anomaly.ZScore:F1}) during {anomaly.KondratievPhase}. " +
                             $"{anomaly.SocraticNote}");
            }

            // Interrogation 5: Kondratiev summer surplus check
            var summers  = report.KondratievAlignment
                .Where(b => b.Phase.Contains("Summer"))
                .Average(b => b.RecordsPerYear);
            var winters  = report.KondratievAlignment
                .Where(b => b.Phase.Contains("Winter"))
                .Average(b => b.RecordsPerYear);

            if (summers > 0 && winters > 0)
            {
                var ratio = summers / winters;
                findings.Add(ratio > 1.2
                    ? $"[SIGNAL] Summer phases produce {ratio:F1}x more records per year than winter phases. " +
                      $"Production density tracks Kondratiev cycle. Candidate invariant — requires Socratic promotion."
                    : $"[INCONCLUSIVE] Summer/winter production ratio is {ratio:F1}x. " +
                      $"Weak signal. Socratic probe: is curation date (acquisition year) " +
                      $"confounding creation date signal?");
            }
        }

        // -----------------------------------------------------------------
        // Render report to console and file
        // -----------------------------------------------------------------
        public static void RenderReport(AuditReport report, string? outputPath = null)
        {
            var sb = new StringBuilder();

            sb.AppendLine("═══════════════════════════════════════════════════════");
            sb.AppendLine("  SELDON ENGINE — CORPUS AUDIT REPORT");
            sb.AppendLine($"  Generated: {DateTime.UtcNow:yyyy-MM-dd HH:mm} UTC");
            sb.AppendLine("═══════════════════════════════════════════════════════");
            sb.AppendLine();

            sb.AppendLine($"Total records:  {report.TotalRecords:N0}");
            sb.AppendLine();

            // Domain breakdown
            sb.AppendLine("── Domain breakdown ──────────────────────────────────");
            foreach (var (domain, count) in report.DomainBreakdown.OrderByDescending(k => k.Value))
                sb.AppendLine($"  {domain,-20} {count,6:N0}  {(double)count/report.TotalRecords,6:P1}");
            sb.AppendLine();

            // Decade distribution with ASCII phase bar
            sb.AppendLine("── Decade distribution with Kondratiev phase ─────────");
            sb.AppendLine($"  {"Decade",-8} {"Count",6} {"Phase",-22} {"Bar"}");
            sb.AppendLine($"  {"──────",-8} {"─────",6} {"─────",-22} {"───"}");

            var maxCount = report.DecadeDistribution.Values
                .Select(b => b.Count).DefaultIfEmpty(1).Max();

            foreach (var (decade, bucket) in report.DecadeDistribution)
            {
                var phase  = KnownPhases
                    .FirstOrDefault(p => decade >= p.Start && decade < p.End);
                var label  = phase?.Name ?? "—";
                var bar    = new string('█', (int)((double)bucket.Count / maxCount * 40));
                var sacred = bucket.Count > 0
                    ? $" ✦{(double)bucket.SacredCount/bucket.Count:P0}"
                    : "";
                sb.AppendLine($"  {decade,-8} {bucket.Count,6:N0} {label,-22} {bar}{sacred}");
            }
            sb.AppendLine();

            // Sacred / secular ratio
            sb.AppendLine("── Sacred / secular ratio by era ─────────────────────");
            sb.AppendLine($"  {"Era",-32} {"Total",7} {"Sacred",7} {"Ratio",7}");
            sb.AppendLine($"  {"───",-32} {"─────",7} {"──────",7} {"─────",7}");
            foreach (var (era, bucket) in report.SacredSecularByEra)
            {
                var ratio = bucket.Total > 0
                    ? (double)bucket.Sacred / bucket.Total
                    : 0;
                var trend = ratio > 0.5 ? "▲" : ratio > 0.25 ? "◆" : "▼";
                sb.AppendLine($"  {era,-32} {bucket.Total,7:N0} {bucket.Sacred,7:N0} {ratio,6:P0} {trend}");
            }
            sb.AppendLine();

            // Kondratiev alignment
            sb.AppendLine("── Kondratiev phase alignment ────────────────────────");
            sb.AppendLine($"  {"Phase",-22} {"Records",8} {"Per yr",7} {"Sacred",7}");
            sb.AppendLine($"  {"─────",-22} {"───────",8} {"──────",7} {"──────",7}");
            foreach (var b in report.KondratievAlignment.Where(b => b.RecordCount > 0))
            {
                var marker = b.Phase.Contains("Summer") ? " ☀" :
                             b.Phase.Contains("Winter") ? " ❄" :
                             b.Phase.Contains("Spring") ? " ↑" : " ↓";
                sb.AppendLine($"  {b.Phase,-22} {b.RecordCount,8:N0} {b.RecordsPerYear,7:F1} {b.SacredRatio,6:P0}{marker}");
            }
            sb.AppendLine();

            // Cultural breadth
            sb.AppendLine("── Cultural breadth (top 15) ─────────────────────────");
            foreach (var (culture, count) in report.CulturalBreakdown.Take(15))
                sb.AppendLine($"  {culture,-28} {count,6:N0}");
            sb.AppendLine();

            // Socratic findings
            sb.AppendLine("── Socratic interrogation findings ───────────────────");
            foreach (var finding in report.SocraticFindings)
            {
                sb.AppendLine();
                sb.AppendLine($"  {finding}");
            }
            sb.AppendLine();

            // Anomalies
            if (report.Anomalies.Any())
            {
                sb.AppendLine("── Statistical anomalies (|z| > 2.0) ────────────────");
                foreach (var a in report.Anomalies)
                    sb.AppendLine($"  {a.Decade}s  z={a.ZScore,+5:F1}  {a.Direction,-8}  {a.KondratievPhase}");
                sb.AppendLine();
            }

            sb.AppendLine("── Next Socratic probes before Layer 2 ───────────────");
            sb.AppendLine("  1. Does production surplus in summer phases survive controlling for");
            sb.AppendLine("     museum acquisition bias? (Museums buy more in boom years too)");
            sb.AppendLine("  2. Is the sacred ratio decline monotonic or stepped?");
            sb.AppendLine("     Stepped = institutional crisis events. Monotonic = slow erosion.");
            sb.AppendLine("  3. What decades show the largest anomalies — are these");
            sb.AppendLine("     civilizational events (wars, revolutions) or data gaps?");
            sb.AppendLine("  4. Do non-Western cultural records show the same decade distribution");
            sb.AppendLine("     as Western records, or do they peak at different phases?");
            sb.AppendLine("     Convergence = universal law. Divergence = Western-specific signal.");
            sb.AppendLine();
            sb.AppendLine("  Nothing in this report is promoted to invariant until these");
            sb.AppendLine("  probes are answered. The Socratic layer precedes the laws.");
            sb.AppendLine();
            sb.AppendLine("═══════════════════════════════════════════════════════");

            var output = sb.ToString();
            Console.WriteLine(output);

            if (outputPath is not null)
            {
                var reportFile = Path.Combine(outputPath, $"corpus_audit_{DateTime.UtcNow:yyyyMMdd_HHmmss}.txt");
                File.WriteAllText(reportFile, output);
                Console.WriteLine($"Report saved: {reportFile}");
            }
        }

        // -----------------------------------------------------------------
        // Load corpus from ndjson files
        // -----------------------------------------------------------------
        private async Task<List<AestheticRecord>> LoadCorpusAsync()
        {
            var records = new List<AestheticRecord>();
            var files   = Directory.GetFiles(corpusPath, "met_corpus_*.ndjson");

            if (files.Length == 0)
            {
                Console.WriteLine($"No corpus files found in {corpusPath}");
                Console.WriteLine("Run MetMuseumIngestPipeline first.");
                return records;
            }

            foreach (var file in files)
            {
                Console.WriteLine($"  Loading {Path.GetFileName(file)}...");
                await foreach (var line in File.ReadLinesAsync(file))
                {
                    if (string.IsNullOrWhiteSpace(line)) continue;
                    try
                    {
                        var record = JsonSerializer.Deserialize<AestheticRecord>(line);
                        if (record is not null) records.Add(record);
                    }
                    catch { /* malformed line — skip */ }
                }
            }

            return records;
        }
    }
}