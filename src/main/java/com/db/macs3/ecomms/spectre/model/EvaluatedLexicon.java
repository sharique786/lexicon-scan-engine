package com.db.macs3.ecomms.spectre.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * One entry in {@code lexicon-hit-summary.evaluated_lexicons} (REPEATED) —
 * represents the outcome of scanning ONE lexicon feature (or one lexicon
 * sub-feature of a composite) against a single message.
 *
 * <p>{@link #totalTermsCount} is the total number of terms compiled into this
 * feature's {@code .hdb} database (from the manifest size — see
 * {@link TermManifestEntry}), regardless of how many actually matched.
 * {@link #regexHitCount} is the number of DISTINCT terms that produced at
 * least one match. {@link #termDtls} contains one entry per term that hit —
 * terms with zero matches are not represented.
 */
public class EvaluatedLexicon implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Parent feature id (STRING) — the composite's id if this came from a sub-feature, else the direct feature's id. */
    private String id;

    /** The lexicon feature/sub-feature name this entry evaluates (matches the .hdb file name). */
    private String name;

    /** Total terms compiled into this feature's .hdb (from manifest size). */
    private long totalTermsCount;

    /** Count of DISTINCT terms that produced at least one match across body + attachments. */
    private long regexHitCount;

    /** One entry per term that hit — empty when {@link #regexHitCount} is 0. */
    private List<TermHitDetail> termDtls = new ArrayList<>();

    public EvaluatedLexicon() {}

    public static EvaluatedLexicon of(String id, String name, long totalTermsCount,
                                       List<TermHitDetail> termDtls) {
        EvaluatedLexicon e = new EvaluatedLexicon();
        e.id              = id;
        e.name            = name;
        e.totalTermsCount = totalTermsCount;
        e.termDtls        = termDtls != null ? termDtls : new ArrayList<>();
        e.regexHitCount   = e.termDtls.size();
        return e;
    }

    public String getId()                      { return id; }
    public void setId(String v)                { this.id = v; }
    public String getName()                    { return name; }
    public void setName(String v)              { this.name = v; }
    public long getTotalTermsCount()           { return totalTermsCount; }
    public void setTotalTermsCount(long v)     { this.totalTermsCount = v; }
    public long getRegexHitCount()             { return regexHitCount; }
    public void setRegexHitCount(long v)       { this.regexHitCount = v; }
    public List<TermHitDetail> getTermDtls()   { return termDtls; }
    public void setTermDtls(List<TermHitDetail> v) {
        this.termDtls = v != null ? v : new ArrayList<>();
        this.regexHitCount = this.termDtls.size();
    }

    /** @return {@code true} when at least one term produced a match. */
    public boolean hasHit() {
        return regexHitCount > 0;
    }

    /**
     * @return a copy of this entry with every {@link TermHitDetail#getMatchedText()}
     *         replaced by the restricted-message placeholder (used for
     *         {@code lexicon-hit-summary} rows where the message is restricted).
     */
    public EvaluatedLexicon withRedactedTermDtls() {
        List<TermHitDetail> redacted = new ArrayList<>(termDtls.size());
        for (TermHitDetail d : termDtls) {
            redacted.add(d.withRedactedText());
        }
        return EvaluatedLexicon.of(id, name, totalTermsCount, redacted);
    }

    @Override
    public String toString() {
        return "EvaluatedLexicon{id='" + id + "', name='" + name + "', hits=" + regexHitCount +
               "/" + totalTermsCount + '}';
    }
}
