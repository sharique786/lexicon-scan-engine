package com.db.macs3.ecomms.spectre.scanengine.gcs;

import com.db.macs3.ecomms.spectre.scanengine.hyperscan.TermIdBuilder;

import java.util.Comparator;
import java.util.List;

/**
 * Resolves the Hyperscan {@code .hdb} file GCS path template:
 * {@code gs://<environment_bkt>/policy_test/<YYYY-MM-DD_HH-MM-SS_<policy_engine_id>>/lex-hyperscan/<feature>.hdb}
 *
 * <p>Confirmed: the {@code policy_test} segment is used regardless of
 * trigger type (live or test), and the timestamp segment is resolved via a
 * GCS wildcard listing rather than being supplied as a runtime parameter.
 *
 * <h2>One listing call per job run, not one per feature</h2>
 * <p>The wildcard timestamp segment is the SAME for every feature a given
 * job run needs — only the trailing {@code <feature>.hdb} differs. This
 * class therefore resolves the wildcard folder ONCE
 * ({@link #resolveBasePath}, a single lightweight GCS metadata listing call)
 * and hands back a base path every feature's {@code .hdb} path is then built
 * from by simple string concatenation ({@link #buildHdbPath}) — avoiding a
 * redundant GCS listing round-trip per feature, which would otherwise scale
 * with the number of DISTINCT features a job run touches (potentially in the
 * hundreds) rather than staying constant.
 *
 * <p>This resolution happens on the DRIVER, once, before any broadcast —
 * see {@code FeatureScanOrchestrator} class Javadoc for why only the small,
 * resolved path strings (never file bytes) are ever broadcast to executors.
 */
public final class HyperscanPathResolver {

    private static final String POLICY_SEGMENT = "policy_test";
    private static final String HDB_SUBFOLDER = "lex-hyperscan";

    /**
     * Lists the immediate child "directory" names one level under a GCS
     * prefix — the one real GCS call this class needs, injected so the
     * wildcard-resolution LOGIC (picking the right child, building paths)
     * can be tested independently of a live GCS client. A real
     * implementation is a thin wrapper over
     * {@code Storage.list(bucket, BlobListOption.prefix(prefix), BlobListOption.currentDirectory())}.
     */
    @FunctionalInterface
    public interface GcsDirectoryLister {
        /** @return child directory names directly under {@code prefix} (not recursive, not full paths) */
        List<String> listImmediateChildDirectories(String bucket, String prefix);
    }

    private HyperscanPathResolver() {}

    /**
     * Resolves {@code gs://<environmentBucket>/policy_test/<resolved-timestamp>_<policyEngineId>/lex-hyperscan/}.
     * When more than one folder matches {@code *_<policyEngineId>} (e.g. left
     * over from a previous run), the LEXICOGRAPHICALLY GREATEST match is used
     * — the {@code YYYY-MM-DD_HH-MM-SS} format sorts lexicographically in
     * chronological order, so this picks the most recent compile.
     *
     * @throws HyperscanFileNotFoundException if no folder matches {@code *_<policyEngineId>} —
     *                                          requirement 3.a: no hyperscan file available must fail
     *                                          the job with a clear error, not proceed silently
     */
    public static String resolveBasePath(String environmentBucket, String policyEngineId, GcsDirectoryLister lister) {
        String prefix = POLICY_SEGMENT + "/";
        List<String> children = lister.listImmediateChildDirectories(environmentBucket, prefix);

        String suffix = "_" + policyEngineId;
        String resolvedFolder = children.stream()
                .filter(name -> name.endsWith(suffix))
                .max(Comparator.naturalOrder())
                .orElseThrow(() -> new HyperscanFileNotFoundException(
                        "No hyperscan compile folder found under gs://" + environmentBucket + "/" + prefix
                        + "matching '*" + suffix + "' — cannot resolve any .hdb file paths for policyEngineId="
                        + policyEngineId + ". Checked " + children.size() + " candidate folder(s)."));

        return "gs://" + environmentBucket + "/" + POLICY_SEGMENT + "/" + resolvedFolder + "/" + HDB_SUBFOLDER + "/";
    }

    /**
     * @param basePath    from {@link #resolveBasePath} — must end with {@code /}
     * @param feature      {@code feature_definition.body.feature}, verbatim
     * @return the full {@code .hdb} path for {@code feature}
     */
    public static String buildHdbPath(String basePath, String feature) {
        if (basePath == null || !basePath.endsWith("/")) {
            throw new IllegalArgumentException("basePath must end with '/', got: " + basePath);
        }
        return basePath + TermIdBuilder.hdbFileName(feature);
    }

    /**
     * @param basePath from {@link #resolveBasePath} — must end with {@code /}
     * @param feature   {@code feature_definition.body.feature}, verbatim
     * @return the full term-metadata JSON path for {@code feature} — see
     *          {@code TermMetadataLoader} class Javadoc for why this is now
     *          resolved and loaded alongside the {@code .hdb} file, matching
     *          the Lexicon Compile Service's own {@code <ruleName>-compile-results.json}
     *          naming convention for the JSON it writes alongside every
     *          {@code /compile/bundle} database (where {@code ruleName} is
     *          this same {@code feature} value).
     */
    public static String buildTermMetadataPath(String basePath, String feature) {
        if (basePath == null || !basePath.endsWith("/")) {
            throw new IllegalArgumentException("basePath must end with '/', got: " + basePath);
        }
        return basePath + TermIdBuilder.termMetadataFileName(feature);
    }

    /** Thrown when no hyperscan compile folder can be resolved — see requirement 3.a. */
    public static final class HyperscanFileNotFoundException extends RuntimeException {
        public HyperscanFileNotFoundException(String message) {
            super(message);
        }
    }
}
