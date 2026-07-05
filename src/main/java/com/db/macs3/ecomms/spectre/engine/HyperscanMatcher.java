package com.db.macs3.ecomms.spectre.engine;

import com.db.macs3.ecomms.spectre.model.TermManifestEntry;
import com.db.macs3.ecomms.spectre.model.TermMatch;
import com.gliwka.hyperscan.wrapper.Database;
import com.gliwka.hyperscan.wrapper.Match;
import com.gliwka.hyperscan.wrapper.Scanner;
import org.apache.spark.broadcast.Broadcast;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Executor-side Hyperscan scanning component.
 *
 * <h2>Database caching (JVM-level, not per-task)</h2>
 * <p>{@link Database} objects are deserialized from broadcast byte arrays on
 * first use and cached in a static {@link ConcurrentHashMap}, shared across
 * all Spark tasks on the same executor JVM. {@code computeIfAbsent} ensures
 * exactly one thread deserializes each database, even under concurrent access.
 *
 * <h2>Manifest-based term identity resolution</h2>
 * <p>Hyperscan's {@code Match.getMatchedExpression().getId()} is a bare
 * integer. To report the human-readable {@code term_id} (e.g.
 * {@code "lexicon_market_cond_2::1"}) and original PCRE {@code term_regex_pattern}
 * required by the output schema, this class consults the feature's
 * {@link TermManifestEntry} map (broadcast alongside the .hdb bytes — see
 * {@link com.db.macs3.ecomms.spectre.reader.GcsHyperscanDatabaseLoader#loadManifests}).
 *
 * <p>If no manifest entry exists for a given expressionId (e.g. the manifest
 * file was missing or out of sync with the .hdb), a synthetic term_id of
 * {@code featureName + "::" + expressionId} is used as a safe fallback so
 * scanning never fails outright due to a missing manifest — it only loses
 * the ability to show the ORIGINAL human-assigned term suffix.
 */
public class HyperscanMatcher implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger log = LoggerFactory.getLogger(HyperscanMatcher.class);

    /** Executor JVM-level cache of deserialized Hyperscan databases, keyed by feature name. */
    private static final ConcurrentHashMap<String, Database> DB_CACHE = new ConcurrentHashMap<>(32);

    /**
     * Scans {@code text} against the Hyperscan database for {@code featureName},
     * resolving each match's human-readable term identity via the manifest.
     *
     * @param featureName        the lexicon feature/sub-feature whose .hdb to scan against
     * @param text               text to scan (message body or one attachment)
     * @param broadcastHdbBytes  broadcast: featureName -> raw .hdb bytes
     * @param broadcastManifests broadcast: featureName -> (expressionId -> TermManifestEntry)
     * @return matches in encounter order, empty if none / no data available
     */
    public List<TermMatch> scan(String featureName,
                                 String text,
                                 Broadcast<Map<String, byte[]>> broadcastHdbBytes,
                                 Broadcast<Map<String, Map<Integer, TermManifestEntry>>> broadcastManifests) {
        if (text == null || text.isBlank()) {
            return List.of();
        }
        Database db = getOrLoadDatabase(featureName, broadcastHdbBytes);
        if (db == null) {
            return List.of();
        }
        Map<Integer, TermManifestEntry> manifest = broadcastManifests.value().get(featureName);
        return performScan(featureName, db, text, manifest);
    }

    private Database getOrLoadDatabase(String featureName, Broadcast<Map<String, byte[]>> broadcastHdbBytes) {
        return DB_CACHE.computeIfAbsent(featureName, name -> {
            byte[] bytes = broadcastHdbBytes.value().get(name);
            if (bytes == null || bytes.length == 0) {
                log.warn("No .hdb bytes found for feature '{}' — skipping scan", name);
                return null;
            }
            try {
                log.info("Deserializing Hyperscan DB for '{}' ({} KB) on executor", name, bytes.length / 1024);
                Database database = Database.load(new ByteArrayInputStream(bytes));
                log.info("Hyperscan DB loaded and cached for '{}'", name);
                return database;
            } catch (IOException e) {
                log.error("Failed to deserialize Hyperscan DB for '{}': {}", name, e.getMessage(), e);
                throw new RuntimeException("Failed to load Hyperscan database for feature: " + name, e);
            }
        });
    }

    private List<TermMatch> performScan(String featureName, Database db, String text,
                                         Map<Integer, TermManifestEntry> manifest) {
        List<TermMatch> results = new ArrayList<>();
        try (Scanner scanner = new Scanner()) {
            scanner.allocScratch(db);
            List<Match> matches = scanner.scan(db, text);
            if (matches.isEmpty()) {
                return results;
            }

            long previousMatchEnd = 0L;
            for (Match match : matches) {
                long start  = match.getStartPosition();
                long end    = match.getEndPosition();
                int exprId  = match.getMatchedExpression() != null ? match.getMatchedExpression().getId() : -1;
                String matchedText = match.getMatchedString();

                String termId;
                String pattern;
                TermManifestEntry entry = manifest != null ? manifest.get(exprId) : null;
                if (entry != null) {
                    termId  = entry.getTermId();
                    pattern = entry.getPattern();
                } else {
                    // Fallback: manifest missing/out of sync — synthesize a term_id so
                    // scanning still proceeds and the miss is visible in the output data.
                    termId  = featureName + "::" + exprId;
                    pattern = null;
                    log.warn("No manifest entry for feature='{}' expressionId={} — using synthetic term_id '{}'",
                             featureName, exprId, termId);
                }

                long delta = results.isEmpty() ? 0L : (start - previousMatchEnd);
                results.add(TermMatch.of(exprId, termId, pattern, matchedText, start, end, delta));
                previousMatchEnd = end;
            }
        } catch (Exception e) {
            log.error("Hyperscan scan error for feature '{}': {}", featureName, e.getMessage(), e);
        }
        return results;
    }

    /** @return the number of databases currently cached on this executor JVM. */
    public static int cachedDatabaseCount() {
        return DB_CACHE.size();
    }

    /** Clears the database cache. For tests only — not called during normal operation. */
    public static void clearCache() {
        DB_CACHE.clear();
        log.info("Hyperscan database cache cleared");
    }
}
