using SeldonEngine.Corpus;
using SeldonEngine.Audit;

// -------------------------------------------------------------------------
// SELDON ENGINE v4 — Entry Point
//
// Socratic precondition log:
//   [OPEN]     Does museum curation bias the civilizational signal?
//   [PARTIAL]  Western bias — broadened corpus ingest reduces from 82%
//   [PENDING]  Europeana cross-cultural falsification
//   [ACTIVE]   Acquisition vs creation bias probe — CompareAcquisitionVsCreation
//
// Command routing:
//   dotnet run -- audit              audit all corpus files
//   dotnet run -- broaden            ingest Medieval, Music, Asian, Islamic, Greek/Roman
//   dotnet run -- dept <id>          ingest single department by ID
//   dotnet run -- europeana <query>  ingest from Europeana (requires EUROPEANA_API_KEY env var)
//   dotnet run                       ingest Modern & Contemporary Art (original run)
// -------------------------------------------------------------------------

var progress = new Progress<IngestProgress>(p =>
{
    var pct = p.Total > 0 ? p.Current * 100 / p.Total : 0;
    Console.Write($"\r[{pct,3}%] {p.Message,-72}");
});

var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    cts.Cancel();
    Console.WriteLine("\nCancellation requested — finishing current batch...");
};

// ── audit ─────────────────────────────────────────────────────────────────
if (args.Length > 0 && args[0] == "audit")
{
    Console.WriteLine("SELDON ENGINE — Corpus Audit");
    Console.WriteLine("Socratic precondition: interrogate before building upward.");
    Console.WriteLine();
    var audit  = new CorpusAudit("./corpus_output");
    var report = await audit.RunAsync();
    CorpusAudit.RenderReport(report, "./corpus_output");
    return;
}

// ── broaden ───────────────────────────────────────────────────────────────
if (args.Length > 0 && args[0] == "broaden")
{
    Console.WriteLine("SELDON ENGINE — Broadened Corpus Ingest");
    Console.WriteLine("Socratic mandate: reduce Western bias, add Music and Architecture domains.");
    Console.WriteLine();
    Console.WriteLine("Targets:");
    foreach (var t in BroadenedMetConfig.Targets)
        Console.WriteLine($"  Dept {t.Id,2}: {t.Name,-28} [{t.DefaultDomain}]");
    Console.WriteLine();

    foreach (var target in BroadenedMetConfig.Targets.OrderBy(t => t.Priority))
    {
        if (cts.Token.IsCancellationRequested) break;
        Console.WriteLine($"── {target.Name} (dept {target.Id}) ──────────────────────");
        Console.WriteLine($"   {target.SocraticNote}");
        Console.WriteLine();

        var source   = new BroadenedMetSource(target);
        var pipeline = new IngestPipeline(source, "./corpus_output", progress);
        await pipeline.IngestAsync(ct: cts.Token);
        Console.WriteLine();
    }

    Console.WriteLine("Broaden complete. Run: dotnet run -- audit");
    return;
}

// ── dept <id> ─────────────────────────────────────────────────────────────
if (args.Length > 1 && args[0] == "dept" && int.TryParse(args[1], out var deptId))
{
    var target = BroadenedMetConfig.Targets.FirstOrDefault(t => t.Id == deptId)
        ?? new DepartmentTarget(deptId, $"Dept{deptId}", "Art", 99,
            "Manual single-department ingest.");

    Console.WriteLine($"Ingesting dept {deptId}: {target.Name}");
    var source   = new BroadenedMetSource(target);
    var pipeline = new IngestPipeline(source, "./corpus_output", progress);
    await pipeline.IngestAsync(ct: cts.Token);
    Console.WriteLine();
    Console.WriteLine("Done. Run: dotnet run -- audit");
    return;
}

// ── europeana <query> ─────────────────────────────────────────────────────
if (args.Length > 1 && args[0] == "europeana")
{
    // Socratic note: Europeana is the cross-cultural falsification layer.
    // The signal it must answer: do non-Western collections show the same
    // decade density peaks as the Met Western corpus?
    // Required env var: EUROPEANA_API_KEY
    // Register free at: https://pro.europeana.eu/page/get-api
    //
    // Recommended first query:
    //   dotnet run -- europeana "(COUNTRY:china OR COUNTRY:japan OR COUNTRY:india) AND TYPE:IMAGE"
    // Then compare decade distribution with Met Western corpus.

    var query    = string.Join(" ", args[1..]);
    Console.WriteLine($"SELDON ENGINE — Europeana Ingest");
    Console.WriteLine($"Query: {query}");
    Console.WriteLine();

    try
    {
        var europeanaSource = new EuropeanaSource(query);
        var pipeline = new IngestPipeline(europeanaSource, "./corpus_output", progress);
        await pipeline.IngestAsync(startYear: 1700, endYear: 2025, ct: cts.Token);
    }
    catch (InvalidOperationException ex)
    {
        Console.WriteLine($"[ERROR] {ex.Message}");
        Console.WriteLine("Set environment variable EUROPEANA_API_KEY before running Europeana ingest.");
        Console.WriteLine("Register free at: https://pro.europeana.eu/page/get-api");
    }

    return;
}

// ── default: original Met ingest ──────────────────────────────────────────
Console.WriteLine("SELDON ENGINE — Met Museum Ingest");
Console.WriteLine("Default: Modern and Contemporary Art (dept 21)");
Console.WriteLine();
Console.WriteLine("Socratic preconditions:");
Console.WriteLine("  1. What would falsify the claim that ornamentation tracks Kondratiev phase?");
Console.WriteLine("  2. Is date imprecision at ±10yr acceptable at 50yr wave scale?");
Console.WriteLine("  3. Does institutional curation (IsHighlight) introduce selection bias?");
Console.WriteLine("  4. Are sacred/secular classifications culturally universal?");
Console.WriteLine();
Console.WriteLine("Press ENTER when ready. Ctrl+C to cancel.");
Console.ReadLine();

// ── Fix: EuropeanaSource was hardcoding API key as env var name ──────────
// Your EuropeanaSource.cs has:
//   private const string ApiKeyEnvVar = "tiffernalk";
// This should be:
//   private const string ApiKeyEnvVar = "EUROPEANA_API_KEY";
// and the actual key stored in that env var, not in source.
// ─────────────────────────────────────────────────────────────────────────

ICorpusSource metSource = new MetSource(departmentId: 21, hasImages: true);
var ingestPipeline   = new IngestPipeline(metSource, "./corpus_output", progress);

try
{
    await ingestPipeline.IngestAsync(startYear: 1700, endYear: 2025, ct: cts.Token);
    Console.WriteLine();
    Console.WriteLine("Ingest complete. Run: dotnet run -- audit");
}
catch (OperationCanceledException)
{
    Console.WriteLine("\nCancelled. Partial output preserved in ./corpus_output/");
}
catch (Exception ex)
{
    Console.WriteLine($"\nFatal: {ex.Message}");
}