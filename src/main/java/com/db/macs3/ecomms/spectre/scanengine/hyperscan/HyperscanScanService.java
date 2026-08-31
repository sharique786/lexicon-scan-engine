package com.db.macs3.ecomms.spectre.scanengine.hyperscan;

import com.db.macs3.ecomms.spectre.scanengine.html.HtmlStrippingService;
import com.db.macs3.ecomms.spectre.scanengine.model.match.AreaMatch;
import com.db.macs3.ecomms.spectre.scanengine.model.match.MatchArea;
import com.db.macs3.ecomms.spectre.scanengine.model.match.MatchSpan;
import com.db.macs3.ecomms.spectre.scanengine.model.match.RawExpressionMatch;
import com.gliwka.hyperscan.wrapper.Database;
import com.gliwka.hyperscan.wrapper.Match;
import com.gliwka.hyperscan.wrapper.Scanner;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Scans one piece of message text (subject, message body, or one
 * attachment's content) against a loaded Hyperscan {@link Database},
 * combining {@link HtmlStrippingService}'s original-text offset mapping
 * with Hyperscan's own match reporting to produce {@link RawExpressionMatch}es
 * at ORIGINAL-text coordinates, grouped by RAW expression id.
 *
 * <h2>Match position semantics (verified against the wrapper's own docs)</h2>
 * <p>{@code com.gliwka.hyperscan-java}'s {@link Match#getStartPosition()} and
 * {@link Match#getEndPosition()} are documented as returning JAVA CHARACTER
 * indices (the wrapper converts Hyperscan's native UTF-8 BYTE offsets to
 * Java {@code char} indices internally) — NOT raw byte offsets requiring a
 * separate conversion step. {@code getEndPosition()} is INCLUSIVE (the index
 * of the last matched character, not one-past-the-end), so it is converted
 * to an exclusive bound ({@code +1}) before being combined with
 * {@link HtmlStrippingService.OffsetMap}, which expects exclusive
 * boundaries. {@link Match#getMatchedString()} (available whenever
 * {@code SOM_LEFTMOST} is set, which this platform always sets for every
 * plain and COMBINATION expression — start-of-match tracking is required
 * for exactly this kind of position reporting) is used directly as the
 * stripped-text matched substring, rather than re-deriving it via
 * {@code String.substring} on the start/end positions, to avoid any risk of
 * an independently-introduced off-by-one disagreeing with what Hyperscan
 * itself reports as matched.
 *
 * <h2>This class no longer resolves expression ids to term identity — confirmed gap fixed</h2>
 * <p>An earlier version of this class took an {@code ExpressionIdResolver}
 * and returned fully-resolved {@code TermMatchResult}s directly, on the
 * documented assumption that every PASS term's reportable expression id was
 * always its own term number, whether or not it needed AND NOT or
 * decomposition. Confirmed BROKEN by the Compile Service's AND NOT fix (see
 * {@code TermExpressionMetadata} class Javadoc for the full explanation):
 * AND NOT terms no longer have that property at all — every required/
 * excluded pattern gets its own individually-allocated id, and resolving
 * one directly via a simple {@code feature::id} builder would silently
 * populate {@code lexicon-hit-summary.term_dtls.term_id} with a wrong,
 * meaningless value, and would report a hit on the excluded pattern
 * matching alone, with no boolean evaluation at all.
 *
 * <p>The fix moves resolution and AND NOT evaluation OUT of this class and
 * up into {@code FeatureScanOrchestrator}, which alone has visibility across
 * every area a feature's scope covers (required and excluded patterns of
 * the SAME AND NOT term can legitimately match in different areas of the
 * same message, so evaluation cannot correctly happen per-area, inside this
 * class). This class's only remaining job is: scan one area, group by RAW
 * expression id, return that — see {@link #scan}.
 *
 * <h2>Caller owns the {@link Scanner} AND the HTML-stripping — both across, not per, scan call</h2>
 * <p>Two performance-critical invariants this class deliberately does NOT
 * enforce itself, because doing so per-call would be the exact anti-pattern
 * they're designed to eliminate:
 * <ul>
 *   <li>The wrapper library's own {@code Scanner} Javadoc: "In case of
 *       multithreaded scanning, you need one scanner instance per CPU
 *       thread" — i.e. ONE instance, reused across every scan that thread
 *       ever does, not one per call. A {@code Scanner} holds a native
 *       function-pointer callback AND scratch space, both real, bounded,
 *       per-JVM-process native resources (the wrapper hard-caps at 256
 *       live instances). {@code FeatureScanOrchestrator} constructs exactly
 *       ONE per Spark partition (matching Spark's own single-thread-per-task
 *       model) and passes it into every {@link #scan} call for that
 *       partition's lifetime — never construct one here.</li>
 *   <li>HTML-stripping (see {@link HtmlStrippingService}) is a function of
 *       the TEXT alone, not the feature being scanned — the same subject/
 *       body/attachment text gets scanned once per applicable lexicon
 *       feature (a message can legitimately have dozens), so stripping it
 *       fresh inside this method on every call would redo the identical
 *       O(n) work that many times over. Callers strip ONCE per message per
 *       area and pass the resulting {@link HtmlStrippingService.StripResult}
 *       in directly.</li>
 * </ul>
 */
public final class HyperscanScanService {

    private HyperscanScanService() {}

    /**
     * Scans {@code stripResult}'s already-stripped text against
     * {@code database} using the caller-owned {@code scanner}, returning one
     * {@link RawExpressionMatch} per DISTINCT raw expression id that
     * matched (each carrying every occurrence found within THIS area,
     * tagged with {@code area}/{@code attachmentId}). No expression-id
     * resolution or AND NOT evaluation happens here — see class Javadoc.
     *
     * @param stripResult   this area's text, already HTML-stripped ONCE per message
     *                       (see class Javadoc) — {@link HtmlStrippingService#strip} for
     *                       subject/body, {@link HtmlStrippingService#identity} for
     *                       attachment {@code cleanText} (never needs stripping)
     * @param database       a loaded Hyperscan database — see {@code HyperscanBundleLoader}
     * @param area             which part of the message this text is
     * @param attachmentId    required (non-null) iff {@code area == ATTACHMENT}; null otherwise
     * @param scanner          the CALLER's own {@link Scanner} instance — reused across every scan
     *                       call for the caller's whole partition/thread lifetime; this method
     *                       calls {@link Scanner#allocScratch} against {@code database} every call
     *                       (cheap/idempotent per the wrapper's own contract — "must be called at
     *                       least once with each database... before scan is called," calling it
     *                       more often than strictly needed is safe) but never constructs or closes
     *                       the {@code Scanner} itself
     * @return raw matches found, empty if {@code stripResult}'s text is blank or nothing matched
     * @throws HyperscanScanException if the native scan call itself fails (scratch allocation,
     *                                  a corrupted database, etc. — see requirement 3.b)
     */
    public static List<RawExpressionMatch> scan(HtmlStrippingService.StripResult stripResult, Database database,
                                                  MatchArea area, String attachmentId, Scanner scanner) {
        String strippedText = stripResult.strippedText();
        if (strippedText == null || strippedText.isBlank()) {
            return List.of();
        }

        List<Match> rawMatches;
        try {
            scanner.allocScratch(database);
            rawMatches = scanner.scan(database, strippedText);
        } catch (Exception e) {
            throw new HyperscanScanException("Hyperscan scan failed: " + e.getMessage(), e);
        }

        if (rawMatches == null || rawMatches.isEmpty()) {
            return List.of();
        }

        // Group raw matches by expression id, preserving first-seen order for determinism.
        Map<Integer, List<Match>> byExpressionId = new LinkedHashMap<>();
        for (Match m : rawMatches) {
            int id = m.getMatchedExpression().getId();
            byExpressionId.computeIfAbsent(id, k -> new ArrayList<>()).add(m);
        }

        List<RawExpressionMatch> results = new ArrayList<>(byExpressionId.size());
        for (Map.Entry<Integer, List<Match>> entry : byExpressionId.entrySet()) {
            int expressionId = entry.getKey();

            // The pattern text comes directly from the match's own Expression object. Every
            // match sharing this expressionId carries the identical Expression instance, so
            // reading it from the first one is sufficient.
            String matchedPatternText = entry.getValue().get(0).getMatchedExpression().getExpression();

            List<AreaMatch> areaMatches = new ArrayList<>(entry.getValue().size());
            for (Match m : entry.getValue()) {
                int strippedStart = (int) m.getStartPosition();
                int strippedEndExclusive = (int) m.getEndPosition() + 1; // inclusive -> exclusive

                int originalStart = stripResult.offsetMap().toOriginal(strippedStart);
                int originalEnd = stripResult.offsetMap().toOriginal(strippedEndExclusive);
                String matchedText = m.getMatchedString();

                MatchSpan span = new MatchSpan(originalStart, originalEnd, matchedText);
                areaMatches.add(new AreaMatch(area, attachmentId, span));
            }
            results.add(new RawExpressionMatch(expressionId, matchedPatternText, areaMatches));
        }
        return results;
    }

    /** Thrown when the native Hyperscan scan call itself fails — see requirement 3.b. */
    public static final class HyperscanScanException extends RuntimeException {
        public HyperscanScanException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
