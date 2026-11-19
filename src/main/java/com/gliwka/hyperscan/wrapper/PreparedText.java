package com.gliwka.hyperscan.wrapper;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * One scanned text's UTF-8 bytes and (for non-ASCII text) its byte-to-char position map, computed at most
 * once. A message's area text is scanned once per applicable feature (often dozens); without this the same
 * O(n) encode and position-map build was redone on every scan. Builds its own position map instead of the
 * library's {@code Utf8}/{@code Utf8Encoder}, which differ between hyperscan-java 5.4.0-2.0.0 and 5.4.11-3.1.0.
 * Not thread-safe — one instance per message, used by one thread.
 */
public final class PreparedText {

    private final String text;
    private final byte[] bytes;
    private int[] positionMap; // lazily built, only when the text is not pure ASCII

    public PreparedText(String text) {
        this.text = text;
        this.bytes = text.getBytes(StandardCharsets.UTF_8);
    }

    public String text() {
        return text;
    }

    byte[] bytes() {
        return bytes;
    }

    boolean isAscii() {
        return bytes.length == text.length();
    }

    int[] positionMap() {
        if (positionMap == null) {
            positionMap = byteToCharMap(text, bytes.length);
        }
        return positionMap;
    }

    /**
     * For every UTF-8 byte of {@code s} (as produced by {@code String.getBytes(UTF_8)}) the index of the char
     * it belongs to; a surrogate pair maps to its high surrogate. An unpaired surrogate is one {@code '?'}
     * byte, exactly like {@code getBytes}.
     */
    static int[] byteToCharMap(String s, int byteLength) {
        int[] map = new int[byteLength];
        int b = 0;
        for (int i = 0, n = s.length(); i < n; ) {
            int cp = s.codePointAt(i);
            int width = cp < 0x80 ? 1 : cp < 0x800 ? 2
                    : cp >= 0xD800 && cp <= 0xDFFF ? 1
                    : cp < 0x10000 ? 3 : 4;
            Arrays.fill(map, b, b + width, i);
            b += width;
            i += Character.charCount(cp);
        }
        return map;
    }
}
