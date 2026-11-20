package com.db.macs3.ecomms.spectre.hyperscan;

import com.gliwka.hyperscan.wrapper.CollapsingScanner;
import com.gliwka.hyperscan.wrapper.Database;
import com.gliwka.hyperscan.wrapper.Expression;
import com.gliwka.hyperscan.wrapper.ExpressionFlag;
import com.gliwka.hyperscan.wrapper.Match;
import com.gliwka.hyperscan.wrapper.TrailingDotStarRewriter;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Real native Hyperscan: a trailing {@code .*} must give one SHORT match per occurrence, not one huge one. */
class CollapsingScannerTest {

    private static final String DISCLAIMER = "This email may contain confidential information.";
    private static final String FILLER = "lorem ipsum dolor sit amet lorem ipsum dolor sit amet lorem ipsum dolor sit amet ";

    private static Database compile(String... patterns) throws Exception {
        List<Expression> expressions = new ArrayList<>();
        for (int i = 0; i < patterns.length; i++) {
            expressions.add(new Expression(patterns[i],
                    EnumSet.of(ExpressionFlag.SOM_LEFTMOST, ExpressionFlag.CASELESS, ExpressionFlag.UTF8), i + 1));
        }
        return TrailingDotStarRewriter.apply(Database.compile(expressions));
    }

    @Test
    void liveCount_tracksOpenScanners_andCloseIsIdempotent() {
        int before = CollapsingScanner.liveCount();
        CollapsingScanner scanner = new CollapsingScanner();
        assertEquals(before + 1, CollapsingScanner.liveCount());

        scanner.close();
        scanner.close(); // second close must not decrement again
        assertEquals(before, CollapsingScanner.liveCount());
    }

    @Test
    void scanAfterClose_failsInsteadOfTouchingFreedNativeMemory() throws Exception {
        try (Database db = compile("abc")) {
            CollapsingScanner scanner = new CollapsingScanner();
            scanner.allocScratch(db);
            scanner.close();
            org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException.class, () -> scanner.scan(db, "abc"));
            org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException.class, () -> scanner.allocScratch(db));
        }
    }

    @Test
    void repeatedScansOfManyTexts_reuseOneScanner() throws Exception {
        // the per-scan native text copy is freed after every scan; a long loop must keep returning correct hits
        try (Database db = compile("needle");
             CollapsingScanner scanner = new CollapsingScanner()) {
            scanner.allocScratch(db);
            for (int i = 0; i < 2_000; i++) {
                List<Match> matches = scanner.scan(db, "hay " + i + " needle hay");
                assertEquals(1, matches.size());
                assertEquals("needle", matches.getFirst().getMatchedString());
            }
        }
    }

    @Test
    void repeatedDisclaimer_threeOccurrences_threeShortMatches() throws Exception {
        String text = FILLER + DISCLAIMER + " " + FILLER + DISCLAIMER + " " + FILLER + DISCLAIMER;

        try (Database db = compile("This email may contain confidential.*");
             CollapsingScanner scanner = new CollapsingScanner()) {
            scanner.allocScratch(db);
            List<Match> matches = scanner.scan(db, text);

            assertEquals(3, matches.size());
            for (Match m : matches) {
                assertEquals(DISCLAIMER, m.getMatchedString());
                assertEquals(DISCLAIMER, text.substring((int) m.getStartPosition(), (int) m.getEndPosition() + 1));
            }
        }
    }

    @Test
    void hugeBodyWithManyDisclaimers_staysBounded() throws Exception {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 2_000; i++) {
            sb.append(FILLER.repeat(20)).append(DISCLAIMER).append(' ');
        }
        try (Database db = compile("This email may contain confidential.*");
             CollapsingScanner scanner = new CollapsingScanner()) {
            scanner.allocScratch(db);
            List<Match> matches = scanner.scan(db, sb.toString());
            assertEquals(2_000, matches.size());
            assertTrue(matches.stream().allMatch(m -> m.getMatchedString().length() == DISCLAIMER.length()));
        }
    }

    @Test
    void unterminatedTail_isCappedAndStopsAtNextOccurrence() throws Exception {
        String text = "This email may contain confidential " + "x".repeat(2_000)
                + " This email may contain confidential tail";
        try (Database db = compile("This email may contain confidential.*");
             CollapsingScanner scanner = new CollapsingScanner()) {
            scanner.allocScratch(db);
            List<Match> matches = scanner.scan(db, text);
            assertEquals(2, matches.size());
            assertTrue(matches.get(0).getMatchedString().length() <= 36 + CollapsingScanner.MAX_TAIL_CHARS);
            assertEquals("This email may contain confidential tail", matches.get(1).getMatchedString());
        }
    }

    @Test
    void nonDotStarPatternsAndNonAsciiPositions_areUnaffected() throws Exception {
        String text = "é日本 start B here";
        try (Database db = compile("B here"); CollapsingScanner scanner = new CollapsingScanner()) {
            scanner.allocScratch(db);
            List<Match> matches = scanner.scan(db, text);
            assertEquals(1, matches.size());
            assertEquals("B here", matches.get(0).getMatchedString());
            assertEquals(text.indexOf("B here"), matches.get(0).getStartPosition());
        }
    }

    @Test
    void noMatch_returnsEmpty() throws Exception {
        try (Database db = compile("zzz.*"); CollapsingScanner scanner = new CollapsingScanner()) {
            scanner.allocScratch(db);
            assertTrue(scanner.scan(db, "nothing here").isEmpty());
        }
    }

    @Test
    void preparedTextScan_isIdenticalToStringScan_andReusableAcrossDatabases() throws Exception {
        String text = "é日本 start B here and again B here";
        try (Database db1 = compile("B here"); Database db2 = compile("start");
             CollapsingScanner scanner = new CollapsingScanner()) {
            com.gliwka.hyperscan.wrapper.PreparedText prepared = new com.gliwka.hyperscan.wrapper.PreparedText(text);
            for (Database db : List.of(db1, db2, db1)) {
                scanner.allocScratch(db);
                scanner.allocScratch(db); // repeat call is a skipped no-op
                List<Match> viaString = scanner.scan(db, text);
                List<Match> viaPrepared = scanner.scan(db, prepared);
                assertEquals(viaString.size(), viaPrepared.size());
                for (int i = 0; i < viaString.size(); i++) {
                    assertEquals(viaString.get(i).getStartPosition(), viaPrepared.get(i).getStartPosition());
                    assertEquals(viaString.get(i).getEndPosition(), viaPrepared.get(i).getEndPosition());
                    assertEquals(viaString.get(i).getMatchedString(), viaPrepared.get(i).getMatchedString());
                }
            }
        }
    }
}
