package com.db.macs3.ecomms.spectre.integration;

import com.db.macs3.ecomms.spectre.model.TermManifestEntry;
import com.gliwka.hyperscan.wrapper.Database;
import com.gliwka.hyperscan.wrapper.Expression;
import com.gliwka.hyperscan.wrapper.ExpressionFlag;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Compiles small Intel Hyperscan databases and their companion manifests
 * entirely in memory, for use in cross-platform integration tests.
 *
 * <h2>Why not ship static .hdb binary fixture files?</h2>
 * <p>Hyperscan's serialised database format is CPU-architecture-specific
 * (see Intel's documentation on {@code hs_serialize_database}) — a
 * {@code .hdb} file compiled on one platform is not guaranteed to load on
 * another. Since the requirement is for integration tests to run identically
 * on Windows, macOS, Linux, and GitHub Actions CI runners, this class instead
 * compiles tiny databases USING THE REAL HYPERSCAN LIBRARY at test run time,
 * on whichever platform the test executes on. The gliwka/hyperscan-java JAR
 * bundles native binaries for all commonly-used developer/CI platforms, so
 * this "compile on demand" approach is genuinely portable where static binary
 * fixtures would not be.
 *
 * <p>Each helper method here mirrors exactly what
 * {@code LexiconCompileService}/{@code HyperscanCompiler} would produce in
 * production, including assigning {@code expressionId = 0-based array index}
 * and building the accompanying {@link TermManifestEntry} map that the real
 * Compile Service publishes as a {@code <featureName>.manifest.json} file.
 */
public final class HdbTestFixtures {

    private HdbTestFixtures() {}

    /**
     * One term definition for building a test lexicon feature.
     *
     * <p>{@code termId} is the human-readable term id, e.g.
     * {@code "lexicon_test_alpha::1"}; {@code pattern} is the PCRE pattern
     * Hyperscan will compile (already "translated" — these tests bypass the
     * operator-language translator entirely and supply raw Hyperscan-compatible
     * patterns directly).
     */
    public static final class TermDef {
        private final String termId;
        private final String pattern;

        public TermDef(String termId, String pattern) {
            this.termId  = termId;
            this.pattern = pattern;
        }

        public String termId()  { return termId; }
        public String pattern() { return pattern; }
    }

    /** Convenience factory for a single-term lexicon feature. */
    public static TermDef term(String termId, String pattern) {
        return new TermDef(termId, pattern);
    }

    /**
     * Result of compiling one test lexicon feature: its serialised .hdb bytes
     * and the manifest mapping expressionId back to termId/pattern.
     */
    public static class CompiledFeature {
        public final String featureName;
        public final byte[] hdbBytes;
        public final Map<Integer, TermManifestEntry> manifest;

        CompiledFeature(String featureName, byte[] hdbBytes, Map<Integer, TermManifestEntry> manifest) {
            this.featureName = featureName;
            this.hdbBytes    = hdbBytes;
            this.manifest    = manifest;
        }
    }

    /**
     * Compiles one lexicon feature's terms into a combined Hyperscan database,
     * mirroring {@code LexiconCompileBundleService.buildBundle()}'s expression-id
     * assignment (0-based array index) and manifest construction.
     *
     * @param featureName the feature/lexicon name (used as the .hdb "file name" key)
     * @param terms       the terms to compile, in order — array index becomes expressionId
     * @return the compiled feature bundle (bytes + manifest)
     */
    public static CompiledFeature compileFeature(String featureName, TermDef... terms) throws Exception {
        Expression[] expressions = new Expression[terms.length];
        Map<Integer, TermManifestEntry> manifest = new LinkedHashMap<>();

        for (int i = 0; i < terms.length; i++) {
            TermDef term = terms[i];
            expressions[i] = new Expression(term.pattern(), EnumSet.of(ExpressionFlag.CASELESS), i);
            manifest.put(i, new TermManifestEntry(i, term.termId(), term.pattern()));
        }

        ArrayList<Expression> expressionList = (ArrayList<Expression>) Arrays.asList(expressions);
        Database db = Database.compile(expressionList);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        db.save(baos);
        db.close();

        return new CompiledFeature(featureName, baos.toByteArray(), manifest);
    }

    /**
     * Compiles several features at once and assembles the two broadcast-ready
     * maps ({@code featureName -> hdbBytes} and {@code featureName -> manifest})
     * that {@link com.db.macs3.ecomms.spectre.engine.HyperscanMatcher} expects.
     *
     * @param features the compiled features to combine
     * @return a pair of maps, accessible via {@link FixtureBundle#hdbBytesByFeature}
     *         and {@link FixtureBundle#manifestsByFeature}
     */
    public static FixtureBundle bundle(CompiledFeature... features) {
        Map<String, byte[]> hdbBytesByFeature = new HashMap<>();
        Map<String, Map<Integer, TermManifestEntry>> manifestsByFeature = new HashMap<>();
        for (CompiledFeature f : features) {
            hdbBytesByFeature.put(f.featureName, f.hdbBytes);
            manifestsByFeature.put(f.featureName, f.manifest);
        }
        return new FixtureBundle(hdbBytesByFeature, manifestsByFeature);
    }

    /** Broadcast-ready pair of maps produced by {@link #bundle}. */
    public static class FixtureBundle {
        public final Map<String, byte[]> hdbBytesByFeature;
        public final Map<String, Map<Integer, TermManifestEntry>> manifestsByFeature;

        FixtureBundle(Map<String, byte[]> hdbBytesByFeature,
                      Map<String, Map<Integer, TermManifestEntry>> manifestsByFeature) {
            this.hdbBytesByFeature  = hdbBytesByFeature;
            this.manifestsByFeature = manifestsByFeature;
        }
    }
}
