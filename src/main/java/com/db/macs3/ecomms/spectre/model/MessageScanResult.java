package com.db.macs3.ecomms.spectre.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Wraps every output row produced for ONE message during a single scan pass.
 *
 * <h2>Why a wrapper</h2>
 * <p>Scanning a message's body and attachments against every applicable
 * lexicon feature is the expensive part of this pipeline. The output,
 * however, must be written to THREE different BigQuery tables
 * ({@code lexicon-hit-summary}, {@code lexicon-hit-restricted},
 * {@code feature-hit-summary}), each with a different row shape. Rather than
 * running {@code mapPartitions} three times (re-scanning the same text three
 * times), {@link com.db.macs3.ecomms.spectre.engine.LexiconScanPartitionFunction}
 * scans ONCE per message and packages every resulting row into this wrapper.
 * The orchestrator then performs three cheap {@code map}/{@code flatMap}
 * extractions over the cached wrapper dataset — no re-scanning required.
 */
public class MessageScanResult implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Always present — one summary row per processed message. */
    private LexiconHitSummaryRow lexiconHitSummaryRow;

    /**
     * Present only when {@code message_type = 'restricted'} AND at least one
     * lexicon feature produced a hit for this message. {@code null} otherwise
     * — unrestricted messages never get a row in {@code lexicon-hit-restricted}.
     */
    private LexiconHitRestrictedRow lexiconHitRestrictedRow;

    /** One entry per (message, feature) or (message, composite, lexicon sub-feature) evaluated. */
    private List<FeatureHitSummaryRow> featureHitSummaryRows = new ArrayList<>();

    public MessageScanResult() {}

    public static MessageScanResult of(LexiconHitSummaryRow summaryRow,
                                        LexiconHitRestrictedRow restrictedRow,
                                        List<FeatureHitSummaryRow> featureRows) {
        MessageScanResult r = new MessageScanResult();
        r.lexiconHitSummaryRow    = summaryRow;
        r.lexiconHitRestrictedRow = restrictedRow;
        r.featureHitSummaryRows   = featureRows != null ? featureRows : new ArrayList<>();
        return r;
    }

    public LexiconHitSummaryRow getLexiconHitSummaryRow()          { return lexiconHitSummaryRow; }
    public void setLexiconHitSummaryRow(LexiconHitSummaryRow v)    { this.lexiconHitSummaryRow = v; }
    public LexiconHitRestrictedRow getLexiconHitRestrictedRow()    { return lexiconHitRestrictedRow; }
    public void setLexiconHitRestrictedRow(LexiconHitRestrictedRow v) { this.lexiconHitRestrictedRow = v; }
    public List<FeatureHitSummaryRow> getFeatureHitSummaryRows()   { return featureHitSummaryRows; }
    public void setFeatureHitSummaryRows(List<FeatureHitSummaryRow> v) {
        this.featureHitSummaryRows = v != null ? v : new ArrayList<>();
    }

    public boolean hasRestrictedRow() {
        return lexiconHitRestrictedRow != null;
    }
}
