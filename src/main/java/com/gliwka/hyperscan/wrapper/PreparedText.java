package com.gliwka.hyperscan.wrapper;

import java.nio.charset.StandardCharsets;

/**
 * One scanned text's UTF-8 bytes and (for non-ASCII text) its byte-to-char position map, computed at most
 * once. A message's area text is scanned once per applicable feature (often dozens); without this the same
 * O(n) encode and position-map build was redone on every scan. Lives in the wrapper's package only because
 * {@code Utf8} is package-private. Not thread-safe — one instance per message, used by one thread.
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
            positionMap = Utf8.byteToStringPositionMap(text, bytes.length);
        }
        return positionMap;
    }
}
