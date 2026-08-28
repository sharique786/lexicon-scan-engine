package com.db.macs3.ecomms.spectre.scanengine.hyperscan;

/**
 * Builds {@code term_id} values: {@code <body.feature>::<index>}, where
 * {@code body.feature} is used VERBATIM (hyphens and all — it is never
 * normalised or re-cased) and {@code index} is the term's position within
 * that feature's compiled Hyperscan database.
 *
 * <p>Example: {@code feature = "lexicon_market_cond-1"}, term at index
 * {@code 1} within that database → {@code term_id = "lexicon_market_cond-1::1"}.
 *
 * <p>The same {@code body.feature} value also names the {@code .hdb} file
 * itself ({@code <body.feature>.hdb>}) — see {@code HyperscanDatabaseLoader}.
 */
public final class TermIdBuilder {

    private static final String SEPARATOR = "::";

    private TermIdBuilder() {}

    /**
     * @param feature    {@code feature_definition.body.feature}, verbatim
     * @param termIndex   the term's position/expression-index within {@code feature}'s
     *                     compiled Hyperscan database
     * @return {@code <feature>::<termIndex>}
     */
    public static String build(String feature, int termIndex) {
        if (feature == null || feature.isBlank()) {
            throw new IllegalArgumentException("feature must not be null/blank when building a term_id");
        }
        return feature + SEPARATOR + termIndex;
    }

    /** @return the {@code .hdb} filename for {@code feature} — {@code <feature>.hdb}. */
    public static String hdbFileName(String feature) {
        if (feature == null || feature.isBlank()) {
            throw new IllegalArgumentException("feature must not be null/blank when building an .hdb filename");
        }
        return feature + ".hdb";
    }

    /**
     * @return the term-metadata JSON filename for {@code feature} —
     *          {@code <feature>-compile-results.json}, matching the Lexicon
     *          Compile Service's own naming convention for the JSON it
     *          writes alongside every {@code /compile/bundle} database (see
     *          {@code TermExpressionMetadata} class Javadoc for why this is
     *          now needed, and {@code TermMetadataLoader} for how it is loaded).
     */
    public static String termMetadataFileName(String feature) {
        if (feature == null || feature.isBlank()) {
            throw new IllegalArgumentException("feature must not be null/blank when building a term-metadata filename");
        }
        return feature + "-compile-results.json";
    }
}
