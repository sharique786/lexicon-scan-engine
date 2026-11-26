package com.db.macs3.ecomms.spectre.hyperscan;

import com.gliwka.hyperscan.wrapper.CollapsingScanner;
import com.gliwka.hyperscan.wrapper.Database;
import com.gliwka.hyperscan.wrapper.Expression;
import com.gliwka.hyperscan.wrapper.ExpressionFlag;
import com.gliwka.hyperscan.wrapper.Match;
import com.gliwka.hyperscan.wrapper.PreparedText;
import com.gliwka.hyperscan.wrapper.TrailingDotStarRewriter;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.EnumSet;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Guards the wrapper-package classes against differences between hyperscan-java 5.4.0-2.0.0 (Windows) and
 * 5.4.13-3.1.1 (Linux/macOS) — see the {@code hyperscan-legacy}/{@code hyperscan-latest} profiles in the pom.
 * Everything here goes through public API or the classes under test, so it must pass on both.
 */
class HyperscanVersionCompatTest {

    private static final EnumSet<ExpressionFlag> FLAGS =
            EnumSet.of(ExpressionFlag.SOM_LEFTMOST, ExpressionFlag.CASELESS, ExpressionFlag.UTF8);

    private static Database compile(String... patterns) throws Exception {
        // List overload: the varargs one does not exist in 5.4.0-2.0.0
        List<Expression> expressions = new java.util.ArrayList<>();
        for (int i = 0; i < patterns.length; i++) {
            expressions.add(new Expression(patterns[i], FLAGS, i + 1));
        }
        return Database.compile(expressions);
    }

    @Test
    void scanOfNonAsciiTextReportsCharIndices_withPreparedText() throws Exception {
        // 2-byte (é), 3-byte (日), 4-byte (emoji, surrogate pair) and an unpaired surrogate before the hit
        String text = "café 日本 😀 lone\uD83D end secret word";
        try (Database db = compile("secret word"); CollapsingScanner scanner = new CollapsingScanner()) {
            scanner.allocScratch(db);
            List<Match> matches = scanner.scan(db, new PreparedText(text));
            assertEquals(1, matches.size());
            Match m = matches.get(0);
            assertEquals(text.indexOf("secret word"), m.getStartPosition());
            assertEquals("secret word", m.getMatchedString());
            assertEquals(text.indexOf("secret word") + "secret word".length() - 1, m.getEndPosition());
        }
    }

    @Test
    void rewriterFromHdbBytes_matchesRewriterFromDatabase() throws Exception {
        Database source = compile("confidential.*", "plain");
        ByteArrayOutputStream hdb = new ByteArrayOutputStream();
        source.save(hdb);
        source.close();

        Database viaBytes = TrailingDotStarRewriter.apply(Database.load(new ByteArrayInputStream(hdb.toByteArray())),
                hdb.toByteArray());
        Database viaSave = TrailingDotStarRewriter.apply(Database.load(new ByteArrayInputStream(hdb.toByteArray())));
        try (viaBytes; viaSave) {
            assertEquals(java.util.Set.of(1), TrailingDotStarRewriter.tailIds(viaBytes));
            assertEquals(java.util.Set.of(1), TrailingDotStarRewriter.tailIds(viaSave));
        }
    }

    @Test
    void databaseWithoutTrailingDotStar_isReturnedUnchanged() throws Exception {
        Database db = compile("plain", "also plain");
        ByteArrayOutputStream hdb = new ByteArrayOutputStream();
        db.save(hdb);
        try (db) {
            assertTrue(TrailingDotStarRewriter.apply(db, hdb.toByteArray()) == db);
            assertTrue(TrailingDotStarRewriter.tailIds(db).isEmpty());
        }
    }

    @Test
    void tailIdsAreTrackedPerDatabaseInstance_notPerContentEquality() throws Exception {
        // Two databases built from the same expressions are equals() to each other; the tail-id registry must
        // still treat them as distinct, so that one being collected never drops the other's entry.
        Database a = TrailingDotStarRewriter.apply(compile("confidential.*"));
        Database b = TrailingDotStarRewriter.apply(compile("confidential.*"));
        try {
            assertEquals(java.util.Set.of(1), TrailingDotStarRewriter.tailIds(a));
            assertEquals(java.util.Set.of(1), TrailingDotStarRewriter.tailIds(b));
            a.close(); // closed exactly once: 5.4.0-2.0.0's close() is not idempotent
            a = null;
            System.gc();
            assertFalse(TrailingDotStarRewriter.tailIds(b).isEmpty());
        } finally {
            if (a != null) {
                a.close();
            }
            b.close();
        }
    }
}
