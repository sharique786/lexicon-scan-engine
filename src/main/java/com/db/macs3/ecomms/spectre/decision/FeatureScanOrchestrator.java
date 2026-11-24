package com.db.macs3.ecomms.spectre.decision;

import com.db.macs3.ecomms.spectre.constants.BqColumns;
import com.db.macs3.ecomms.spectre.html.ChatVoiceMessageTextExtractor;
import com.db.macs3.ecomms.spectre.html.HtmlStrippingService;
import com.db.macs3.ecomms.spectre.hyperscan.CollapsingScannerPool;
import com.db.macs3.ecomms.spectre.hyperscan.HyperscanBundleLoader;
import com.db.macs3.ecomms.spectre.hyperscan.HyperscanScanService;
import com.db.macs3.ecomms.spectre.hyperscan.TermIdBuilder;
import com.db.macs3.ecomms.spectre.model.feature.FeatureDefinition;
import com.db.macs3.ecomms.spectre.model.match.AreaMatch;
import com.db.macs3.ecomms.spectre.model.match.MatchArea;
import com.db.macs3.ecomms.spectre.model.match.MatchSpan;
import com.db.macs3.ecomms.spectre.model.match.RawExpressionMatch;
import com.db.macs3.ecomms.spectre.model.match.TermMatchResult;
import com.db.macs3.ecomms.spectre.model.message.ChannelType;
import com.db.macs3.ecomms.spectre.model.message.MessageAttachment;
import com.db.macs3.ecomms.spectre.model.message.MessageSource;
import com.db.macs3.ecomms.spectre.model.message.ScanMessage;
import com.db.macs3.ecomms.spectre.model.termmeta.TermExpressionMetadata;
import com.db.macs3.ecomms.spectre.model.termmeta.TermExpressionMetadata.TermEntry;
import com.db.macs3.ecomms.spectre.model.view.FeatureDecisionRow;
import com.gliwka.hyperscan.wrapper.Database;
import com.gliwka.hyperscan.wrapper.CollapsingScanner;
import com.gliwka.hyperscan.wrapper.Scanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Scans one {@link FeatureDecisionRow} (one lexicon feature applied to one
 * message) against whatever areas its {@code feature_definition.body.scope}
 * covers, using a per-partition {@link HyperscanBundleLoader} for the
 * {@code .hdb} database and the accompanying per-term AND NOT/decomposition
 * metadata — both extracted from the same GCS zip bundle, see that class's
 * Javadoc. This is the real, Hyperscan/GCS-backed
 * {@link DecisionTreeEvaluator.FeatureRowScanner} implementation.
 *
 * <h2>Designed for reuse inside one {@code mapPartitions} call</h2>
 * <p>One instance is constructed per Spark partition (sharing that
 * partition's single {@link HyperscanBundleLoader}, and therefore its
 * cache), and its processMessage() is called once per message within
 * that partition. Nothing here performs a Spark action — every call operates
 * on exactly one message and returns plain, serialisable result objects,
 * safe to run entirely on an executor.
 *
 * <h2>Term id resolution and AND NOT evaluation</h2>
 * <p>A matched Hyperscan expression id is not always the term's own number
 * — see {@link TermExpressionMetadata} class Javadoc for the full id
 * scheme. {@code term_id} is always resolved via {@link TermIdBuilder},
 * never built directly from a raw matched expression id.
 *
 * <h2>Scan every area first, then resolve/evaluate once, across all of them</h2>
 * <p>{@link HyperscanScanService#scan} returns raw, un-resolved
 * {@link RawExpressionMatch}es per area. This class scans every area the
 * feature's scope covers, merges the raw results by expression id ACROSS
 * all of them (required and excluded patterns of the same AND NOT term can
 * legitimately match in different areas of the same message — a per-area
 * evaluation would miss this), then for every DISTINCT term any matched
 * expression id belongs to (via {@link TermExpressionMetadata}), evaluates:
 * the required side is satisfied iff EVERY entry of
 * {@code requiredExpressionIds} appears in the combined match-id set; for
 * an AND NOT term, the excluded side is satisfied (term therefore excluded)
 * iff EVERY entry of {@code excludedExpressionIds} also appears — same AND
 * convention on both sides. A term only produces a {@link TermMatchResult}
 * when the required side is satisfied AND the excluded side is not.
 *
 * <p>{@code term_regex_pattern} prefers {@link TermExpressionMetadata}'s own
 * pattern text over the raw match's expression text, since for a
 * COMBINATION match the expression text is the unreadable boolean formula
 * itself (e.g. {@code "(2&3&4)"}), not the term's actual pattern.
 *
 * <h2>Two evaluation paths, chosen per term</h2>
 * <ul>
 *   <li><b>Id presence, merged across areas</b> ({@link #resolveAndEvaluateCrossArea}) — a term without a
 *       {@code resolvedPatternTree}: a simple term or an inline-compiled proximity term (one id; Hyperscan already verified
 *       the whole condition), or an id-list AND NOT term. Required ids must all be present and, for AND NOT, the excluded
 *       ids must not all be present, judged over the matches of EVERY scanned area.</li>
 *   <li><b>Per-area Java evaluation</b> ({@link #resolveAndEvaluatePerArea}) — a term with a {@code resolvedPatternTree}
 *       ({@code NEAR}/{@code FOLLOWEDBY}, plain AND, AND NOT, or a single {@code resolvedPatterns} regex). The term's
 *       {@code hyperscanExpressionId}, when present, is only a cheap pre-filter (globally and per area); the real condition
 *       is checked by {@link ResolvedPatternAreaEvaluator} against that ONE area's original text and is never merged
 *       across areas, because word distance is meaningless across two different texts.</li>
 * </ul>
 *
 * <h2>Performance: one {@link Scanner}, and one HTML-strip pass, per message — never per feature</h2>
 * <p>A message can legitimately be evaluated against dozens of applicable
 * lexicon features (one {@link #scanRow} call each, all via the SAME
 * {@link #scannerFor} closure). Two anti-patterns this class deliberately
 * avoids, both scaling with (features × messages), not just messages:
 * <ul>
 *   <li><b>Constructing a new {@code Scanner} per scan call.</b> The wrapper
 *       library's own {@code Scanner} Javadoc: "you need one scanner
 *       instance per CPU thread" (reused across every scan that thread ever
 *       does) — it holds a native function-pointer callback and scratch
 *       space, both real, capped, per-JVM-process native resources (a
 *       hard limit of 256 live instances). This class owns exactly ONE
 *       {@link Scanner} for its entire lifetime (one instance = one Spark
 *       partition, matching Spark's own single-thread-per-task model) —
 *       see {@link #close}.</li>
 *   <li><b>Re-running {@code HtmlStrippingService.strip} per feature.</b>
 *       Subject/body/attachment text doesn't change across which feature is
 *       being scanned — only the {@code Database} does. {@link #scannerFor}
 *       strips every area's text EXACTLY ONCE per message
 *       ({@link #precomputeAreaTexts}), and every {@link #scanRow} call for
 *       that message reuses the same precomputed
 *       {@link HtmlStrippingService.StripResult}s. Attachment
 *       {@code attachmentText} uses {@link HtmlStrippingService#identity} rather
 *       than {@link HtmlStrippingService#strip} — see
 *       {@code MessageAttachment} class Javadoc: it is already HTML-free by
 *       the time it reaches this engine, so the real stripping algorithm
 *       (and its {@code int[]} offset-map allocation) would be pure waste
 *       on text that can legitimately be megabytes long.</li>
 * </ul>
 *
 * <h2>MESSAGE_BODY stripping is channel-specific — see {@link #messageBodyStripResult}</h2>
 * <p>EMAIL {@code msg_text} is stripped directly, as it always has been. CHAT/VOICE
 * {@code msg_text} is an HTML table report where only the {@code <td>} cell(s) whose
 * {@code class} carries the {@code message_text} token are real message content — every other
 * column (room name, event type, email, attachment metadata, ...) must never reach Hyperscan.
 * {@link #messageBodyStripResult} routes to {@code ChatVoiceMessageTextExtractor} for those two
 * channels, falling back to the EMAIL-style direct strip when no {@code message_text} cell is
 * found at all (e.g. a CHAT/VOICE message that isn't this table shape).
 */
public final class FeatureScanOrchestrator implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(FeatureScanOrchestrator.class);

    private final HyperscanBundleLoader bundleLoader;
    private final Long maxAttachmentSizeBytes;
    private final CollapsingScanner scanner;

    /**
     * @param bundleLoader           this partition's shared, cached zip-bundle loader — see
     *                               {@link HyperscanBundleLoader} class Javadoc
     * @param maxAttachmentSizeBytes null means unlimited — an attachment whose
     *                               {@code attachmentText} UTF-8 byte length exceeds this is
     *                               skipped entirely (not scanned, not an error)
     */
    public FeatureScanOrchestrator(HyperscanBundleLoader bundleLoader, Long maxAttachmentSizeBytes) {
        this(bundleLoader, maxAttachmentSizeBytes, CollapsingScanner.DEFAULT_MAX_TAIL_CHARS);
    }

    /**
     * @param maxTailChars cap on how far a term ending in {@code .*} (e.g. a disclaimer) is extended past its
     *                     prefix; the tail otherwise stops at the end of the line — see {@link CollapsingScanner}
     */
    public FeatureScanOrchestrator(HyperscanBundleLoader bundleLoader, Long maxAttachmentSizeBytes, int maxTailChars) {
        this(bundleLoader, maxAttachmentSizeBytes, maxTailChars, null);
    }

    /**
     * @param scannerPool when non-null, the scanner is BORROWED from this executor-wide pool and given back on
     *                    {@link #close()} instead of being created and closed here — production must use the pool,
     *                    see {@link CollapsingScannerPool} for the JVM crash per-task scanners caused
     */
    public FeatureScanOrchestrator(HyperscanBundleLoader bundleLoader, Long maxAttachmentSizeBytes, int maxTailChars,
                                   CollapsingScannerPool scannerPool) {
        this.bundleLoader = bundleLoader;
        this.maxAttachmentSizeBytes = maxAttachmentSizeBytes;
        this.scannerPool = scannerPool;
        this.scanner = scannerPool != null ? scannerPool.borrow(maxTailChars) : new CollapsingScanner(maxTailChars);
    }

    /** Pool the scanner was borrowed from, or null when this instance owns (and closes) it. */
    private final CollapsingScannerPool scannerPool;
    /** close() runs twice in production (iterator end + task-completion listener); a pooled scanner must be returned once. */
    private final java.util.concurrent.atomic.AtomicBoolean closed = new java.util.concurrent.atomic.AtomicBoolean();

    /**
     * A {@link DecisionTreeEvaluator.FeatureRowScanner} bound to one message —
     * strips every area's text exactly once here (see class Javadoc), then
     * reuses that across however many {@link #scanRow} calls the caller
     * makes for this same message (one per applicable feature).
     */
    public DecisionTreeEvaluator.FeatureRowScanner scannerFor(ScanMessage message) {
        // A partition handles one message at a time: starting the next one ends the previous one, so free the
        // previous message's native text copies now (see MessageAreaText#prepared). Safe even if a caller still
        // used an older scanner afterwards — PreparedText just makes a new native copy on its next scan.
        releaseNativeTexts();
        List<MessageAreaText> areaTexts = precomputeAreaTexts(message);
        currentMessageAreaTexts = areaTexts;
        return row -> scanRow(row, areaTexts);
    }

    /** Area texts of the message most recently passed to {@link #scannerFor}; their native copies are freed next. */
    private List<MessageAreaText> currentMessageAreaTexts = List.of();

    private void releaseNativeTexts() {
        for (MessageAreaText areaText : currentMessageAreaTexts) {
            areaText.releaseNative();
        }
        currentMessageAreaTexts = List.of();
    }

    /**
     * One area's text, stripped exactly once per message — see class Javadoc.
     */
    private static final class MessageAreaText {
        private final MatchArea area;
        private final String attachmentId;
        private final String originalText;
        private final HtmlStrippingService.StripResult stripResult;
        private com.gliwka.hyperscan.wrapper.PreparedText prepared; // lazy, built on first scan
        private ResolvedPatternAreaEvaluator.AreaText evaluatorText; // lazy, shared by every proximity term

        MessageAreaText(MatchArea area, String attachmentId, String originalText,
                        HtmlStrippingService.StripResult stripResult) {
            this.area = area;
            this.attachmentId = attachmentId;
            this.originalText = originalText;
            this.stripResult = stripResult;
        }

        MatchArea area() { return area; }
        String attachmentId() { return attachmentId; }
        String originalText() { return originalText; }
        HtmlStrippingService.StripResult stripResult() { return stripResult; }

        /** The original text with its word index, shared by every proximity term of every feature this message. */
        ResolvedPatternAreaEvaluator.AreaText evaluatorText() {
            if (evaluatorText == null) {
                evaluatorText = new ResolvedPatternAreaEvaluator.AreaText(originalText);
            }
            return evaluatorText;
        }

        /**
         * UTF-8 encoding of the stripped text, computed once and reused for every feature scanned — including its
         * NATIVE copy, so a large attachment is copied off-heap once per message, not once per feature. Freed by
         * {@link #releaseNative()} when the message is done.
         */
        com.gliwka.hyperscan.wrapper.PreparedText prepared() {
            if (prepared == null) {
                prepared = new com.gliwka.hyperscan.wrapper.PreparedText(stripResult.strippedText());
            }
            return prepared;
        }

        void releaseNative() {
            if (prepared != null) {
                prepared.close();
            }
        }
    }

    private List<MessageAreaText> precomputeAreaTexts(ScanMessage message) {
        List<MessageAreaText> areaTexts = new ArrayList<>(2 + message.attachmentsOrEmpty().size());

        String subject = message.getContent() == null ? null : message.getContent().getSubject();
        if (subject != null && !subject.isBlank()) {
            areaTexts.add(new MessageAreaText(MatchArea.SUBJECT, null, subject, HtmlStrippingService.strip(subject)));
        }

        String msgText = message.getContent() == null ? null : message.getContent().getMsgText();
        if (msgText != null && !msgText.isBlank()) {
            areaTexts.add(new MessageAreaText(MatchArea.MESSAGE_BODY, null, msgText, messageBodyStripResult(message, msgText)));
        }

        for (MessageAttachment attachment : message.attachmentsOrEmpty()) {
            MessageAreaText attachmentAreaText = toEligibleAttachmentAreaText(attachment);
            if (attachmentAreaText != null) {
                areaTexts.add(attachmentAreaText);
            }
        }
        return areaTexts;
    }

    /**
     * Resolves the MESSAGE_BODY area's {@link HtmlStrippingService.StripResult} for {@code msgText},
     * per {@link ChannelType} — see {@code ChatVoiceMessageTextExtractor} class Javadoc for why
     * CHAT/VOICE need a different strategy than the plain {@link HtmlStrippingService#strip} the
     * EMAIL path has always used.
     *
     * <h2>EMAIL — unchanged</h2>
     * <p>Strips {@code msgText} directly, exactly as before this method existed.
     *
     * <h2>CHAT/VOICE — extract {@code message_text} cells first, then strip</h2>
     * <p>{@code msgText} for these channels is an HTML table report where most columns (room
     * name, event type, email, attachment metadata, ...) are never scannable message content —
     * only the {@code <td>} cell(s) whose {@code class} carries the {@code message_text} token
     * do. Scanning the WHOLE table would feed Hyperscan every other column's text too, risking a
     * lexicon match on content that was never part of the actual message. {@link ChatVoiceMessageTextExtractor#extract}
     * pulls just those cells out (with an offset map back to {@code msgText}), and
     * {@link HtmlStrippingService#stripExtracted} then strips any further HTML those cells
     * themselves carry (e.g. a {@code <p>}-wrapped message) while composing that offset map with
     * its own — so a match's position always resolves back to {@code msgText}, never to the
     * intermediate, cells-only text.
     *
     * <h2>Fallback: not every CHAT/VOICE message is that table shape</h2>
     * <p>When no {@code message_text} cell is found at all (including a {@code msgText} that
     * isn't this table shape to begin with — e.g. a plain-text test fixture, or a CHAT/VOICE
     * message this engine hasn't seen the shape of yet), this falls back to stripping
     * {@code msgText} directly, same as EMAIL — never silently drops content that could still
     * legitimately be scanned.
     */
    private static HtmlStrippingService.StripResult messageBodyStripResult(ScanMessage message, String msgText) {
        MessageSource source = message.getSource();
        ChannelType channelType = ChannelType.fromChannelName(source == null ? null : source.getChannelName());
        if (channelType == ChannelType.CHAT || channelType == ChannelType.VOICE) {
            ChatVoiceMessageTextExtractor.ExtractionResult extraction = ChatVoiceMessageTextExtractor.extract(msgText);
            if (extraction.anyCellFound()) {
                return HtmlStrippingService.stripExtracted(extraction.extractedText(), extraction.offsetMap());
            }
            log.debug("channelType={}: no message_text <td> cell found in msg_text — "
                    + "falling back to whole-text HTML stripping", channelType);
        }
        return HtmlStrippingService.strip(msgText);
    }

    /**
     * @return the attachment's area text, or null if it's oversized or has no scannable content.
     */
    private MessageAreaText toEligibleAttachmentAreaText(MessageAttachment attachment) {
        if (!withinSizeLimit(attachment)) {
            return null;
        }
        String attachmentText = attachment.getAttachmentText();
        if (attachmentText == null || attachmentText.isBlank()) {
            return null;
        }
        // identity(), not strip(): attachment attachmentText is already HTML-free — see class Javadoc.
        return new MessageAreaText(MatchArea.ATTACHMENT, attachment.getAttachmentId(), attachmentText,
                HtmlStrippingService.identity(attachmentText));
    }

    /**
     * @return the scope constant (see {@code BqColumns.FeatureDefinitionJson}) a given area is gated by.
     */
    private static String scopeFor(MatchArea area) {
        return switch (area) {
            case SUBJECT -> BqColumns.FeatureDefinitionJson.SCOPE_SUBJECT;
            case MESSAGE_BODY -> BqColumns.FeatureDefinitionJson.SCOPE_MESSAGE_BODY;
            case ATTACHMENT -> BqColumns.FeatureDefinitionJson.SCOPE_ATTACHMENT;
        };
    }

    /**
     * Scans one row's lexicon feature against every precomputed area its
     * scope covers, merges raw matches for the SAME expression id found in
     * more than one area, then resolves and evaluates every distinct term
     * referenced — see class Javadoc.
     */
    private List<TermMatchResult> scanRow(FeatureDecisionRow row, List<MessageAreaText> areaTexts) {
        FeatureDefinition definition = FeatureDefinition.parseCached(row.getFeatureDefinitionJson());
        String feature = definition.getBody().getLexiconName();
        HyperscanBundleLoader.LexiconBundle bundle = bundleLoader.load(feature);
        Database database = bundle.database();
        TermExpressionMetadata metadata = bundle.metadata();

        List<RawExpressionMatch> allRawMatches = new ArrayList<>();
        List<AreaScanContext> areaScans = new ArrayList<>();

        for (MessageAreaText areaText : areaTexts) {
            if (!definition.getBody().hasScope(scopeFor(areaText.area()))) {
                continue;
            }
            String stripped = areaText.stripResult().strippedText();
            List<RawExpressionMatch> matches = HyperscanScanService.scan(
                    areaText.stripResult(),
                    stripped == null || stripped.isBlank() ? null : areaText.prepared(),
                    database, areaText.area(), areaText.attachmentId(), scanner);
            allRawMatches.addAll(matches);
            areaScans.add(new AreaScanContext(areaText.area(), areaText.attachmentId(), areaText.originalText(), matches,
                    areaText));
        }

        return resolveAndEvaluate(feature, metadata, allRawMatches, areaScans);
    }

    /**
     * One scanned area's raw matches AND its real original text — captured
     * here, at the scan call site, because it is not threaded through
     * {@link RawExpressionMatch}/{@link AreaMatch} today. Needed only by
     * terms with a {@code resolvedPatternTree} (see
     * {@link TermExpressionMetadata.TermEntry#requiresPerAreaEvaluation()}),
     * which must be verified against this SAME area's real text —
     * {@link ResolvedPatternAreaEvaluator} never receives text spanning more
     * than one area.
     */
    private record AreaScanContext(MatchArea area, String attachmentId, String originalText,
                                   List<RawExpressionMatch> rawMatches, MessageAreaText source) {
    }

    /**
     * Releases this instance's single {@link Scanner} — call once, when this
     * partition's processing is entirely done (see {@code PartitionProcessor}'s
     * try-with-resources block). Never close and keep using the same
     * instance for more messages afterward.
     */
    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        releaseNativeTexts();
        if (scannerPool != null) {
            scannerPool.release(scanner); // stays open for the next task — never close a pooled scanner
        } else {
            scanner.close();
        }
    }

    private boolean withinSizeLimit(MessageAttachment attachment) {
        String text = attachment.getAttachmentText();
        if (maxAttachmentSizeBytes == null || text == null) {
            return true;
        }
        long limit = maxAttachmentSizeBytes;
        int chars = text.length();
        // UTF-8 needs between 1 and 3 bytes per UTF-16 char (a surrogate pair is 2 chars -> 4 bytes), so
        // chars > limit can never fit and chars <= limit/3 always fits; count exactly (no byte[]) only between.
        return chars <= limit && (chars <= limit / 3 || utf8Length(text) <= limit);
    }

    /** Exact UTF-8 byte length, matching {@code String.getBytes(UTF_8)} (unpaired surrogate -> 1 byte '?'). */
    static long utf8Length(CharSequence s) {
        long bytes = 0;
        for (int i = 0, n = s.length(); i < n; ) {
            int codePoint = Character.codePointAt(s, i);
            i += Character.charCount(codePoint);
            bytes += utf8Bytes(codePoint);
        }
        return bytes;
    }

    private static int utf8Bytes(int codePoint) {
        boolean unpairedSurrogate = codePoint >= 0xD800 && codePoint <= 0xDFFF; // 1 '?' byte, like getBytes
        if (codePoint < 0x80 || unpairedSurrogate) {
            return 1;
        }
        if (codePoint < 0x800) {
            return 2;
        }
        return codePoint < 0x10000 ? 3 : 4;
    }

    /**
     * Merges raw matches ACROSS every scanned area by expression id, then
     * resolves and evaluates every distinct term referenced — see class
     * Javadoc "The fix: scan every area first, THEN resolve/evaluate once".
     *
     * <p>Terms with a {@code resolvedPatternTree} (see
     * {@link TermEntry#requiresPerAreaEvaluation()}) are evaluated by an
     * entirely separate branch below, PER SCANNED AREA, never merged into
     * this cross-area id-presence logic — word-distance/order is only
     * meaningful within one contiguous text. Do not "simplify" by folding
     * that branch into this merge; a required word in the subject and an
     * excluded word in the body legitimately share this cross-area boolean
     * check for the id-list AND NOT scheme, but do NOT share a
     * coordinate space for a proximity/AND-NOT condition evaluated via
     * {@link ResolvedPatternAreaEvaluator} — conflating the two would
     * silently reintroduce a false-positive class of bug.
     */
    private List<TermMatchResult> resolveAndEvaluate(String feature, TermExpressionMetadata metadata,
                                                     List<RawExpressionMatch> allRawMatches,
                                                     List<AreaScanContext> areaScans) {
        Map<Integer, List<AreaMatch>> matchesByExpressionId = new LinkedHashMap<>();
        Map<Integer, String> patternTextByExpressionId = new LinkedHashMap<>();
        for (RawExpressionMatch rawMatch : allRawMatches) {
            matchesByExpressionId.computeIfAbsent(rawMatch.getExpressionId(), unusedKey -> new ArrayList<>())
                    .addAll(rawMatch.getMatches());
            patternTextByExpressionId.putIfAbsent(rawMatch.getExpressionId(), rawMatch.getMatchedPatternText());
        }
        Set<Integer> matchedExpressionIds = matchesByExpressionId.keySet();
        Map<Integer, TermEntry> termsToEvaluate = collectTermsToEvaluate(feature, metadata, matchedExpressionIds);

        // Each area's matched-id set is needed by every per-area term; build it once per area here (only if some
        // term needs it), not once per term x area. Index-aligned with areaScans.
        List<Set<Integer>> idsByArea = null;
        for (TermEntry entry : termsToEvaluate.values()) {
            if (entry.requiresPerAreaEvaluation()) {
                idsByArea = new ArrayList<>(areaScans.size());
                for (AreaScanContext areaScan : areaScans) {
                    Set<Integer> ids = new HashSet<>();
                    for (RawExpressionMatch raw : areaScan.rawMatches()) {
                        ids.add(raw.getExpressionId());
                    }
                    idsByArea.add(ids);
                }
                break;
            }
        }

        List<TermMatchResult> results = new ArrayList<>();
        for (TermEntry entry : termsToEvaluate.values()) {
            TermMatchResult termMatchResult = buildTermMatchResult(
                    feature, entry, matchedExpressionIds, matchesByExpressionId, patternTextByExpressionId, areaScans,
                    idsByArea);
            if (termMatchResult != null) {
                results.add(termMatchResult);
            }
        }
        return results;
    }

    /**
     * Every DISTINCT term any matched expression id belongs to, in first-seen order for
     * determinism — a term may be referenced by several matched ids (an AND NOT term's
     * required AND excluded sides both matching), so dedupe by term number. Also includes
     * every mandatory-per-area term (a resolvedPatternTree term with NO expression id at
     * all — a mandatory-per-area AND NOT term the Compile Service gave no id list for),
     * which is otherwise never discoverable via a matched expression id.
     */
    private Map<Integer, TermEntry> collectTermsToEvaluate(String feature, TermExpressionMetadata metadata,
                                                           Set<Integer> matchedExpressionIds) {
        Map<Integer, TermEntry> termsToEvaluate = new LinkedHashMap<>();
        Set<Integer> unrecognisedIds = new LinkedHashSet<>();
        for (int expressionId : matchedExpressionIds) {
            TermEntry entry = metadata.termByAnyExpressionId(expressionId);
            if (entry != null) {
                termsToEvaluate.putIfAbsent(entry.getTermNumber(), entry);
            } else {
                unrecognisedIds.add(expressionId);
            }
        }
        if (!unrecognisedIds.isEmpty()) {
            log.warn("feature='{}': {} matched expression id(s) not found in term metadata — skipping: {}",
                    feature, unrecognisedIds.size(), unrecognisedIds);
        }
        for (TermEntry entry : metadata.mandatoryPerAreaTerms()) {
            termsToEvaluate.putIfAbsent(entry.getTermNumber(), entry);
        }
        return termsToEvaluate;
    }

    /**
     * @return this term's resolved match result, or null if the term did not produce any matches.
     */
    private TermMatchResult buildTermMatchResult(String feature, TermEntry entry, Set<Integer> matchedExpressionIds,
                                                 Map<Integer, List<AreaMatch>> matchesByExpressionId,
                                                 Map<Integer, String> patternTextByExpressionId,
                                                 List<AreaScanContext> areaScans,
                                                 List<Set<Integer>> idsByArea) {
        List<AreaMatch> combined = entry.requiresPerAreaEvaluation()
                ? resolveAndEvaluatePerArea(entry, matchedExpressionIds, areaScans, idsByArea)
                : resolveAndEvaluateCrossArea(feature, entry, matchedExpressionIds, matchesByExpressionId);
        if (combined.isEmpty()) {
            return null;
        }

        String termId = TermIdBuilder.build(feature, entry.getTermNumber());
        // Prefer the metadata's own pattern text (the resolvedPatterns string, or the
        // Compile Service's translatedPattern — always a genuine, readable pattern) over the
        // raw match's Expression text, which for a COMBINATION match is the unreadable
        // boolean formula string itself, and for an AND NOT match is only ONE leaf's own
        // text, not representative of the whole term.
        String termRegexPattern = entry.getTermRegexPattern() != null
                ? entry.getTermRegexPattern()
                : getTermRegexPattern(entry, patternTextByExpressionId);

        return new TermMatchResult(termId, termRegexPattern, entry.getTermDescription(), combined);
    }

    private String getTermRegexPattern(TermEntry entry, Map<Integer, String> patternTextByExpressionId) {
        return entry.hasCoarseExpressionId()
                ? patternTextByExpressionId.get(entry.getRequiredExpressionIds().getFirst())
                : null;
    }

    /**
     * Today's cross-area, id-presence-only evaluation — unchanged for any term without a resolvedPatternTree.
     */
    private List<AreaMatch> resolveAndEvaluateCrossArea(String feature, TermEntry entry,
                                                        Set<Integer> matchedExpressionIds,
                                                        Map<Integer, List<AreaMatch>> matchesByExpressionId) {
        boolean requiredSatisfied = matchedExpressionIds.containsAll(entry.getRequiredExpressionIds());
        boolean excludedSatisfied = entry.isRequiresExclusionCheck()
                && entry.getExcludedExpressionIds() != null
                && !entry.getExcludedExpressionIds().isEmpty()
                && matchedExpressionIds.containsAll(entry.getExcludedExpressionIds());

        if (!requiredSatisfied || excludedSatisfied) {
            if (entry.isRequiresExclusionCheck() && requiredSatisfied) {
                log.debug("feature='{}', term={}: excluded by AND NOT — required side matched but so did "
                        + "the excluded side", feature, entry.getTermNumber());
            }
            return List.of();
        }

        // Combine matches from EVERY required-side expression id — the excluded side is
        // never itself surfaced as a "hit" to report, only a condition already applied above.
        List<AreaMatch> combined = new ArrayList<>();
        for (int reqId : entry.getRequiredExpressionIds()) {
            combined.addAll(matchesByExpressionId.getOrDefault(reqId, List.of()));
        }
        return combined;
    }

    /**
     * New per-area evaluation for a term with a {@code resolvedPatternTree} —
     * see class Javadoc on {@link #resolveAndEvaluate} for why this must
     * never merge areas together. The term's coarse
     * {@code hyperscanExpressionId} (when available) is used only as a cheap
     * pre-filter, both globally and per area — never as the actual condition.
     */
    private List<AreaMatch> resolveAndEvaluatePerArea(TermEntry entry,
                                                      Set<Integer> matchedExpressionIds,
                                                      List<AreaScanContext> areaScans,
                                                      List<Set<Integer>> idsByArea) {
        if (entry.hasCoarseExpressionId() && !matchedExpressionIds.containsAll(entry.getRequiredExpressionIds())) {
            return List.of(); // global pre-filter: at least one required leaf never matched anywhere at all
        }

        List<AreaMatch> combined = new ArrayList<>();
        for (int i = 0; i < areaScans.size(); i++) {
            AreaScanContext areaScan = areaScans.get(i);
            if (!canSatisfyCondition(entry, areaScan, idsByArea.get(i))) {
                continue;
            }
            List<MatchSpan> spans = ResolvedPatternAreaEvaluator.findMatchingSpans(
                    entry.getResolvedPatternTree(), areaScan.source().evaluatorText());
            for (MatchSpan span : spans) {
                combined.add(new AreaMatch(areaScan.area(), areaScan.attachmentId(), span));
            }
        }
        return combined;
    }

    /**
     * @return false if this area's text is empty, or (pre-filter) is missing a required leaf entirely.
     */
    private static boolean canSatisfyCondition(TermEntry entry, AreaScanContext areaScan,
                                               Set<Integer> areaExpressionIds) {
        if (areaScan.originalText() == null || areaScan.originalText().isBlank()) {
            return false;
        }
        if (!entry.hasCoarseExpressionId()) {
            return true;
        }
        return areaExpressionIds.containsAll(entry.getRequiredExpressionIds());
    }
}
