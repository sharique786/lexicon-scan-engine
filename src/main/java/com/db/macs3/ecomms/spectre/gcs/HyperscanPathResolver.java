package com.db.macs3.ecomms.spectre.gcs;

import com.db.macs3.ecomms.spectre.hyperscan.TermIdBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.List;

/**
 * Resolves the Lexicon Compile Service's per-feature zip bundle GCS path:
 * {@code gs://<hdb-gcs-bucket>/<hdb-gcs-prefix>/<YYYY-MM-DD_HH-MM-SS>_<policy_engine_id>/output/lex-hyperscan/<feature>.zip}
 *
 * <p>{@code hdb-gcs-bucket}/{@code hdb-gcs-prefix} come from the {@link com.db.macs3.ecomms.spectre.config.DataprocConfig}
 * {@code spectre.engine.hyperscan} section. The timestamp folder is resolved by listing the prefix rather
 * than being supplied as a runtime parameter.
 *
 * <p>Each zip contains the compiled {@code .hdb} and its compile-results JSON — see
 * {@code HyperscanBundleLoader}.
 *
 * <h2>One listing call per job run, not one per feature</h2>
 * <p>The timestamp folder is the SAME for every feature a run needs; only the trailing
 * {@code <feature>.zip} differs. {@link #resolveBasePath} therefore lists once (a single GCS metadata
 * call) and {@link #buildZipPath} builds each feature's path by string concatenation, so GCS listing cost
 * does not grow with the number of features.
 *
 * <p>Resolution happens on the driver before the broadcast — only the small resolved path strings (never
 * file bytes) go to the executors.
 */
public final class HyperscanPathResolver {

    private static final Logger log = LoggerFactory.getLogger(HyperscanPathResolver.class);

    private static final String HDB_SUBFOLDER = "lex-hyperscan";
    private static final String OUTPUT_SUBFOLDER = "output";

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
        /**
         * @return child directory names directly under {@code prefix} (not recursive, not full paths)
         */
        List<String> listImmediateChildDirectories(String bucket, String prefix);
    }

    private HyperscanPathResolver() {
    }

    /**
     * Resolves {@code gs://<hdbGcsBucket>/<hdbGcsPrefix>/<resolved-timestamp>_<policyEngineId>/lex-hyperscan/}.
     * When more than one folder matches {@code *_<policyEngineId>} (e.g. left
     * over from a previous run), the LEXICOGRAPHICALLY GREATEST match is used
     * — the {@code YYYY-MM-DD_HH-MM-SS} format sorts lexicographically in
     * chronological order, so this picks the most recent compile.
     *
     * @param hdbGcsBucket {@code DataprocConfig.hyperscan().hdbGcsBucket()}
     * @param hdbGcsPrefix {@code DataprocConfig.hyperscan().hdbGcsPrefix()} — e.g. {@code "policy_test"}
     * @throws HyperscanFileNotFoundException if no folder matches {@code *_<policyEngineId>}
     */
    public static String resolveBasePath(String hdbGcsBucket, String hdbGcsPrefix, String policyEngineId,
                                         GcsDirectoryLister lister) {
        String prefix = hdbGcsPrefix + "/";
        log.info("Resolving Hyperscan base path under gs://{}/{} for policyEngineId={}",
                hdbGcsBucket, prefix, policyEngineId);
        List<String> children = lister.listImmediateChildDirectories(hdbGcsBucket, prefix);

        String suffix = "_" + policyEngineId;
        String resolvedFolder = children.stream()
                .filter(name -> name.endsWith(suffix))
                .max(Comparator.naturalOrder())
                .orElseThrow(() -> new HyperscanFileNotFoundException(
                        "No hyperscan compile folder found under gs://" + hdbGcsBucket + "/" + prefix
                                + "matching '*" + suffix + "' — cannot resolve any .hdb file paths for policyEngineId="
                                + policyEngineId + ". Checked " + children.size() + " candidate folder(s)."));

        String basePath = "gs://" + hdbGcsBucket + "/" + hdbGcsPrefix + "/" + resolvedFolder
                + "/" + OUTPUT_SUBFOLDER + "/" + HDB_SUBFOLDER + "/";
        log.info("Resolved Hyperscan base path for policyEngineId={}: {} (chose '{}' among {} candidate folder(s))",
                policyEngineId, basePath, resolvedFolder, children.size());
        return basePath;
    }

    /**
     * @param basePath from {@link #resolveBasePath} — must end with {@code /}
     * @param feature  {@code feature_definition.body.lexiconName}, verbatim
     * @return the full zip bundle path for {@code feature} — see class Javadoc
     */
    public static String buildZipPath(String basePath, String feature) {
        if (basePath == null || !basePath.endsWith("/")) {
            throw new IllegalArgumentException("basePath must end with '/', got: " + basePath);
        }
        return basePath + TermIdBuilder.zipFileName(feature);
    }

    /**
     * Thrown when no hyperscan compile folder can be resolved for a policy engine id.
     */
    public static final class HyperscanFileNotFoundException extends RuntimeException {
        public HyperscanFileNotFoundException(String message) {
            super(message);
        }
    }
}
