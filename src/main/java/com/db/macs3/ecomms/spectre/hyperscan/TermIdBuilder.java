package com.db.macs3.ecomms.spectre.hyperscan;

/**
 * Builds {@code term_id} values and the names of a feature's bundle files. A {@code term_id} is
 * {@code <body.lexiconName>::<termNumber>}: {@code body.lexiconName} is used VERBATIM (hyphens and all —
 * never normalised or re-cased) and {@code termNumber} is the term's own number, the {@code ::<n>} suffix
 * of its {@code termId} in the compile-results JSON. It is NOT necessarily the Hyperscan expression id
 * that matched — see {@code TermExpressionMetadata}.
 *
 * <p>Example: {@code feature = "lexicon_market_cond-1"}, term number {@code 1} →
 * {@code "lexicon_market_cond-1::1"}.
 *
 * <p>The same {@code body.lexiconName} names the GCS bundle ({@code <lexiconName>.zip}) and the two entries
 * inside it ({@link #hdbFileName}, {@link #termMetadataFileName}) — see {@code HyperscanBundleLoader}.
 */
public final class TermIdBuilder {

    private static final String SEPARATOR = "::";

    private TermIdBuilder() {
    }

    /**
     * @param feature   {@code feature_definition.body.lexiconName}, verbatim
     * @param termIndex the term's own number within {@code feature} (the {@code ::<n>} suffix of its
     *                  {@code termId} in the compile-results JSON)
     * @return {@code <feature>::<termIndex>}
     */
    public static String build(String feature, int termIndex) {
        if (feature == null || feature.isBlank()) {
            throw new IllegalArgumentException("feature must not be null/blank when building a term_id");
        }
        return feature + SEPARATOR + termIndex;
    }

    /**
     * @return the {@code .hdb} entry name expected INSIDE {@code feature}'s zip bundle — {@code <feature>.hdb}.
     */
    public static String hdbFileName(String feature) {
        if (feature == null || feature.isBlank()) {
            throw new IllegalArgumentException("feature must not be null/blank when building an .hdb filename");
        }
        return feature + ".hdb";
    }

    /**
     * @return the term-metadata JSON entry name expected INSIDE {@code feature}'s zip bundle —
     * {@code <feature>.json}, the Lexicon Compile Service's compile-results file (parsed by
     * {@code TermExpressionMetadata}, loaded by {@code HyperscanBundleLoader}).
     */
    public static String termMetadataFileName(String feature) {
        if (feature == null || feature.isBlank()) {
            throw new IllegalArgumentException("feature must not be null/blank when building a term-metadata filename");
        }
        return feature + ".json";
    }

    /**
     * @return the GCS zip bundle filename for {@code feature} — {@code <feature>.zip} — the single
     * file the Compile Service writes per feature, containing BOTH {@link #hdbFileName} and
     * {@link #termMetadataFileName}. See {@code HyperscanPathResolver#buildZipPath}.
     */
    public static String zipFileName(String feature) {
        if (feature == null || feature.isBlank()) {
            throw new IllegalArgumentException("feature must not be null/blank when building a zip filename");
        }
        return feature + ".zip";
    }
}
