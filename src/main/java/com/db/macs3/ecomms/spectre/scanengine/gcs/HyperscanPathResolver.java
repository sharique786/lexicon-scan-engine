package com.db.macs3.ecomms.spectre.scanengine.gcs;

import com.db.macs3.ecomms.spectre.scanengine.hyperscan.TermIdBuilder;

import java.util.Comparator;
import java.util.List;

/**
 * Resolves the Lexicon Compile Service's per-feature zip bundle GCS path
 * template:
 * {@code gs://<hdb-gcs-bucket>/<hdb-gcs-prefix>/<YYYY-MM-DD_HH-MM-SS_<policy_engine_id>>/lex-hyperscan/<feature>.zip}
 *
 * <p>{@code hdb-gcs-bucket}/{@code hdb-gcs-prefix} come from the
 * {@code DataprocConfig} YAML's {@code spectre.engine.hyperscan} section
 * (see that class) — the {@code policy_test} segment was previously a
 * hardcoded constant here, used regardless of trigger type (live or test);
 * it is now environment-supplied instead, since a folder naming convention
 * baked into this engine's own source was never really a constant, just
 * previously unconfigurable. The timestamp segment is still resolved via a
 * GCS wildcard listing rather than being supplied as a runtime parameter.
 *
 * <p>Each feature's zip bundle contains both the compiled {@code .hdb} and
 * its {@code compile-results.json} metadata as entries — see
 * {@code HyperscanBundleLoader} for how that zip is downloaded once and both
 * entries extracted from it.
 *
 * <h2>One listing call per job run, not one per feature</h2>
 * <p>The wildcard timestamp segment is the SAME for every feature a given
 * job run needs — only the trailing {@code <feature>.zip} differs. This
 * class therefore resolves the wildcard folder ONCE
 * ({@link #resolveBasePath}, a single lightweight GCS metadata listing call)
 * and hands back a base path every feature's zip path is then built from by
 * simple string concatenation ({@link #buildZipPath}) — avoiding a redundant
 * GCS listing round-trip per feature, which would otherwise scale with the
 * number of distinct features a job run touches rather than staying
 * constant.
 *
 * <p>This resolution happens on the driver, once, before any broadcast — only
 * the small, resolved path strings (never file bytes) are ever broadcast to
 * executors.
 */
public final class HyperscanPathResolver {

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
     * Resolves {@code gs://<hdbGcsBucket>/<hdbGcsPrefix>/<resolved-timestamp>_<policyEngineId>/lex-hyperscan/}.
     * When more than one folder matches {@code *_<policyEngineId>} (e.g. left
     * over from a previous run), the LEXICOGRAPHICALLY GREATEST match is used
     * — the {@code YYYY-MM-DD_HH-MM-SS} format sorts lexicographically in
     * chronological order, so this picks the most recent compile.
     *
     * @param hdbGcsBucket   {@code DataprocConfig.hyperscan().hdbGcsBucket()}
     * @param hdbGcsPrefix   {@code DataprocConfig.hyperscan().hdbGcsPrefix()} — e.g. {@code "policy_test"}
     * @throws HyperscanFileNotFoundException if no folder matches {@code *_<policyEngineId>}
     */
    public static String resolveBasePath(String hdbGcsBucket, String hdbGcsPrefix, String policyEngineId,
                                          GcsDirectoryLister lister) {
        String prefix = hdbGcsPrefix + "/";
        List<String> children = lister.listImmediateChildDirectories(hdbGcsBucket, prefix);

        String suffix = "_" + policyEngineId;
        String resolvedFolder = children.stream()
                .filter(name -> name.endsWith(suffix))
                .max(Comparator.naturalOrder())
                .orElseThrow(() -> new HyperscanFileNotFoundException(
                        "No hyperscan compile folder found under gs://" + hdbGcsBucket + "/" + prefix
                        + "matching '*" + suffix + "' — cannot resolve any .hdb file paths for policyEngineId="
                        + policyEngineId + ". Checked " + children.size() + " candidate folder(s)."));

        return "gs://" + hdbGcsBucket + "/" + hdbGcsPrefix + "/" + resolvedFolder + "/" + HDB_SUBFOLDER + "/";
    }

    /**
     * @param basePath    from {@link #resolveBasePath} — must end with {@code /}
     * @param feature      {@code feature_definition.body.feature}, verbatim
     * @return the full zip bundle path for {@code feature} — see class Javadoc
     */
    public static String buildZipPath(String basePath, String feature) {
        if (basePath == null || !basePath.endsWith("/")) {
            throw new IllegalArgumentException("basePath must end with '/', got: " + basePath);
        }
        return basePath + TermIdBuilder.zipFileName(feature);
    }

    /** Thrown when no hyperscan compile folder can be resolved for a policy engine id. */
    public static final class HyperscanFileNotFoundException extends RuntimeException {
        public HyperscanFileNotFoundException(String message) {
            super(message);
        }
    }
}
