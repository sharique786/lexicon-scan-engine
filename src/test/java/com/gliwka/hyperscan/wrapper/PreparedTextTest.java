package com.gliwka.hyperscan.wrapper;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link PreparedText}'s own byte-to-char map must agree byte for byte with {@code String.getBytes(UTF_8)}, which is
 * what is actually handed to Hyperscan — any drift shifts every reported match position after it.
 */
@DisplayName("PreparedText")
class PreparedTextTest {

    @ParameterizedTest
    @ValueSource(strings = {
            "plain ascii",
            "café über",                 // 2-byte
            "甲乙 שוק",       // 3-byte CJK + Hebrew (2-byte)
            "a𠀀b",                        // 4-byte surrogate pair
            "x\uD800y",                              // unpaired high surrogate -> one '?' byte
            "\uDC00",                                // unpaired low surrogate alone
            ""
    })
    @DisplayName("maps every UTF-8 byte to the char it came from, matching getBytes(UTF_8) exactly")
    void mapMatchesJdkEncoding(String text) {
        byte[] jdkBytes = text.getBytes(StandardCharsets.UTF_8);
        PreparedText prepared = new PreparedText(text);

        assertThat(prepared.bytes()).isEqualTo(jdkBytes);
        // Expected map: encode each code point on its own (exactly as getBytes does inside the whole string) and
        // attribute every resulting byte to the index of that code point's first char.
        int[] expected = new int[jdkBytes.length];
        int b = 0;
        for (int i = 0; i < text.length(); ) {
            int cp = text.codePointAt(i);
            int width = new String(Character.toChars(cp)).getBytes(StandardCharsets.UTF_8).length;
            for (int k = 0; k < width; k++) {
                expected[b++] = i;
            }
            i += Character.charCount(cp);
        }
        assertThat(b).isEqualTo(jdkBytes.length);
        assertThat(PreparedText.byteToCharMap(text, jdkBytes.length)).containsExactly(expected);
    }

    @Test
    @DisplayName("pure ASCII text needs no position map")
    void asciiIsDetected() {
        assertThat(new PreparedText("hello").isAscii()).isTrue();
        assertThat(new PreparedText("héllo").isAscii()).isFalse();
    }

    @Test
    @DisplayName("the position map is built once and reused")
    void positionMapIsCached() {
        PreparedText prepared = new PreparedText("café");
        assertThat(prepared.positionMap()).isSameAs(prepared.positionMap());
        assertThat(prepared.positionMap()).containsExactly(0, 1, 2, 3, 3);
    }

    @Test
    @DisplayName("the native copy is made once and reused; the heap copy is dropped but bytes() still re-encodes")
    void nativeCopyIsReusedAndHeapCopyDropped() {
        String text = "café 日本";
        byte[] jdkBytes = text.getBytes(StandardCharsets.UTF_8);
        try (PreparedText prepared = new PreparedText(text)) {
            assertThat(prepared.hasNativeCopy()).isFalse();
            org.bytedeco.javacpp.BytePointer first = prepared.nativeBytes();
            assertThat(prepared.nativeBytes()).isSameAs(first);
            assertThat(prepared.hasNativeCopy()).isTrue();

            byte[] copied = new byte[jdkBytes.length];
            first.get(copied);
            assertThat(copied).isEqualTo(jdkBytes);
            assertThat(prepared.bytes()).isEqualTo(jdkBytes);       // re-encoded on demand
            assertThat(prepared.byteLength()).isEqualTo(jdkBytes.length);
        }
    }

    @Test
    @DisplayName("close() frees the native copy, is idempotent, and a later use makes a fresh copy")
    void closeIsIdempotentAndRecoverable() {
        PreparedText prepared = new PreparedText("hello");
        prepared.nativeBytes();
        prepared.close();
        prepared.close();
        assertThat(prepared.hasNativeCopy()).isFalse();
        byte[] again = new byte[5];
        prepared.nativeBytes().get(again);
        assertThat(again).isEqualTo("hello".getBytes(StandardCharsets.UTF_8));
        prepared.close();
    }

    @Test
    @DisplayName("scanning one PreparedText against several databases keeps ONE native copy with identical results")
    void scanReusesNativeCopyAcrossDatabases() {
        java.util.EnumSet<ExpressionFlag> flags = java.util.EnumSet.of(ExpressionFlag.SOM_LEFTMOST, ExpressionFlag.UTF8);
        String text = "é日本 start B here and again B here";
        try (Database db1 = Database.compile(java.util.List.of(new Expression("B here", flags, 1)));
             Database db2 = Database.compile(java.util.List.of(new Expression("start", flags, 1)));
             CollapsingScanner scanner = new CollapsingScanner();
             PreparedText prepared = new PreparedText(text)) {
            scanner.allocScratch(db1);
            java.util.List<Match> first = scanner.scan(db1, prepared);
            org.bytedeco.javacpp.BytePointer nativeCopy = prepared.nativeBytes();
            scanner.allocScratch(db2);
            java.util.List<Match> second = scanner.scan(db2, prepared);
            assertThat(prepared.nativeBytes()).isSameAs(nativeCopy);  // not re-copied for the second database

            assertThat(first).extracting(Match::getMatchedString).containsExactly("B here", "B here");
            assertThat(first.getFirst().getStartPosition()).isEqualTo(text.indexOf("B here"));
            assertThat(second).extracting(Match::getMatchedString).containsExactly("start");
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }
}
