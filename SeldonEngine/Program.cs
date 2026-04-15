using SeldonEngine.Corpus;

// -------------------------------------------------------------------------
// SELDON ENGINE — Entry Point
// Layer 1: Aesthetic Corpus Ingest — Metropolitan Museum of Art
//
// Socratic precondition log:
//   [OPEN]    Does museum curation bias the civilizational signal?
//   [OPEN]    Is 470k objects sufficient statistical mass at Kondratiev scale?
//   [OPEN]    Does the Met's Western bias invalidate cross-cultural claims?
//   [PENDING] Falsification: run same pipeline on Europeana — do signals diverge?
//
// These questions are not obstacles. They are the engine's immune system.
// An invariant that cannot survive them was never an invariant.
// -------------------------------------------------------------------------

var cts      = new CancellationTokenSource();
var progress = new Progress<IngestProgress>(p =>
{
    var pct = p.Total > 0 ? (p.Current * 100 / p.Total) : 0;
    Console.Write($"\r[{pct,3}%] {p.Message,-70}");
});

Console.WriteLine("SELDON ENGINE — Layer 1 Aesthetic Corpus Ingest");
Console.WriteLine("================================================");
Console.WriteLine("Source: Metropolitan Museum of Art Open Access API");
Console.WriteLine();
Console.WriteLine("Socratic preconditions:");
Console.WriteLine("  Before this data becomes signal, ask:");
Console.WriteLine("  1. What would falsify the claim that ornamentation tracks Kondratiev phase?");
Console.WriteLine("  2. Is date imprecision at ±10yr acceptable at 50yr wave scale?");
Console.WriteLine("  3. Does institutional curation (IsHighlight) introduce selection bias?");
Console.WriteLine("  4. Are sacred/secular classifications culturally universal?");
Console.WriteLine();

// -------------------------------------------------------------------------
// Note on the 403 issue:
//
// The previous version placed Task.Delay(RateLimitMs) AFTER semaphore.Release(),
// meaning the delay ran in parallel with the next request rather than before it.
// This caused requests to burst with no throttle, triggering IP-level blocks.
//
// The fix: delay is now inside the try block, BEFORE semaphore.Release().
// Rate limit is also reduced from 10,000ms to 250ms (4 req/s) — still polite,
// but ~40x faster. Explicit 403/429/503 handlers add hard back-off on throttle signals.
//
// If you're resuming after a 403 block, wait ~10 minutes before re-running.
// -------------------------------------------------------------------------

Console.WriteLine("Press ENTER when ready to begin ingest. Ctrl+C to cancel.");
Console.ReadLine();

Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    cts.Cancel();
    Console.WriteLine("\nCancellation requested — finishing current batch...");
};

var pipeline = new MetMuseumIngestPipeline(
    outputPath: "./corpus_output",
    progress:   progress
);

// -------------------------------------------------------------------------
// RECOMMENDED FIRST RUN: European Paintings (departmentId = 11)
// ~2,500 works · 1400–1900 · cleanest date metadata · strong Kondratiev coverage
//
// Full corpus run (all 470k objects) takes ~4–6 hours at 250ms rate limit
// Run full corpus overnight after first-run validation succeeds
//
// Department IDs for reference:
//   1  = American Decorative Arts
//   3  = Ancient Near Eastern Art
//   4  = Arms and Armor
//   6  = Asian Art
//   7  = The Costume Institute
//   8  = Drawings and Prints
//   9  = Egyptian Art
//   10 = European Decorative Arts
//   11 = European Paintings          ← START HERE
//   12 = European Sculpture
//   13 = Greek and Roman Art
//   14 = Islamic Art
//   15 = The Robert Lehman Collection
//   17 = Medieval Art
//   18 = Musical Instruments         ← SECOND RUN
//   19 = Photographs
//   21 = Modern and Contemporary Art
// -------------------------------------------------------------------------

try
{
    await pipeline.IngestAsync(
        departmentId: 11,   // European Paintings — first signal layer
        startYear:    1700,     // Wave 1 onset
        endYear:      2025,     // Present
        hasImages:    true,
        ct:           cts.Token
    );

    Console.WriteLine();
    Console.WriteLine();
    Console.WriteLine("Ingest complete.");
    Console.WriteLine("Output: ./corpus_output/*.ndjson");
    Console.WriteLine();
    Console.WriteLine("Next steps:");
    Console.WriteLine("  1. Inspect output — verify date distribution covers Kondratiev waves");
    Console.WriteLine("  2. Run sacred/secular classification audit — sample 100 records manually");
    Console.WriteLine("  3. Run Musical Instruments department (id=18) for music layer");
    Console.WriteLine("  4. Cross-check: does record count per decade correlate with known boom periods?");
    Console.WriteLine("     (More art produced in Kondratiev summers — this itself is a signal)");
    Console.WriteLine();
    Console.WriteLine("Socratic checkpoint before proceeding to vector layer:");
    Console.WriteLine("  Q: Does the decade distribution of works confirm or challenge Kondratiev periodicity?");
    Console.WriteLine("  Q: Is the sacred/secular ratio trend visible in raw metadata alone?");
    Console.WriteLine("  Q: What anomalies appear that the model does not predict?");
}
catch (OperationCanceledException)
{
    Console.WriteLine("\nIngest cancelled. Partial output preserved in ./corpus_output/");
}
catch (Exception ex)
{
    Console.WriteLine($"\nFatal error: {ex.Message}");
    Console.WriteLine(ex.StackTrace);
}