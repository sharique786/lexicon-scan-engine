package com.gliwka.hyperscan.wrapper;

import org.bytedeco.javacpp.BytePointer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * One scanned text's UTF-8 bytes and (for non-ASCII text) its byte-to-char position map, computed at most
 * once. A message's area text is scanned once per applicable feature (often dozens); without this the same
 * O(n) encode and position-map build was redone on every scan. Builds its own position map instead of the
 * library's {@code Utf8}/{@code Utf8Encoder}, which differ between hyperscan-java 5.4.0-2.0.0 and 5.4.11-3.1.0.
 *
 * <h2>Native copy, shared across scans</h2>
 * {@code hs_scan} needs the text in NATIVE memory. {@link #nativeBytes()} makes that copy once and every later
 * scan of this text (one per feature) reuses it, instead of copying a possibly multi-MB attachment again for
 * every feature. Once the native copy exists the heap {@code byte[]} is dropped, so the text is held in
 * UTF-8 form only once. The OWNER must call {@link #close()} when the text will not be scanned again (for an
 * area text: when its message is done) — off-heap memory is invisible to GC pressure. {@code close()} is
 * idempotent and NOT final: a later scan just makes a new native copy, so an early close can never cause a
 * use-after-free, only an extra copy.
 *
 * <p>Not thread-safe — one instance per message, used by one thread.
 */
public final class PreparedText implements AutoCloseable {

    private final String text;
    private final int byteLength;
    private byte[] bytes;              // heap UTF-8; null while a native copy exists (re-encoded if needed again)
    private BytePointer nativeBytes;   // lazily created, released by close()
    private int[] positionMap;         // lazily built, only when the text is not pure ASCII

    public PreparedText(String text) {
        this.text = text;
        this.bytes = text.getBytes(StandardCharsets.UTF_8);
        this.byteLength = bytes.length;
    }

    public String text() {
        return text;
    }

    /** UTF-8 bytes of the text (re-encoded if the heap copy was dropped for a native one). */
    byte[] bytes() {
        return bytes != null ? bytes : text.getBytes(StandardCharsets.UTF_8);
    }

    int byteLength() {
        return byteLength;
    }

    boolean isAscii() {
        return byteLength == text.length();
    }

    int[] positionMap() {
        if (positionMap == null) {
            positionMap = byteToCharMap(text, byteLength);
        }
        return positionMap;
    }

    /** The text's UTF-8 bytes in native memory, copied on first use and reused until {@link #close()}. */
    BytePointer nativeBytes() {
        if (nativeBytes == null) {
            // Wrapping a heap array makes JavaCPP copy it into a fresh NATIVE allocation.
            //nativeBytes = new BytePointer(ByteBuffer.wrap(bytes()));
            BytePointer buffer = new BytePointer(byteLength + 64L);
            buffer.put(bytes(), 0, byteLength);
            buffer.position(byteLength).put(new byte[64], 0, 64).position(0);
            nativeBytes = buffer;
            bytes = null; // the native copy now holds the bytes; don't keep a second (heap) copy for the message
        }
        return nativeBytes;
    }

    /** True while a native copy is held (diagnostics/tests). */
    boolean hasNativeCopy() {
        return nativeBytes != null;
    }

    /** Frees the native copy, if any. Idempotent; a later scan simply makes a new copy. */
    @Override
    public void close() {
        if (nativeBytes != null) {
            nativeBytes.close();
            nativeBytes = null;
        }
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
            int width = utf8Width(cp);
            Arrays.fill(map, b, b + width, i);
            b += width;
            i += Character.charCount(cp);
        }
        return map;
    }

    /** UTF-8 byte width of a code point; an unpaired surrogate is one '?' byte, like {@code getBytes}. */
    private static int utf8Width(int cp) {
        if (cp < 0x80 || isSurrogate(cp)) {
            return 1;
        }
        if (cp < 0x800) {
            return 2;
        }
        return cp < 0x10000 ? 3 : 4;
    }

    private static boolean isSurrogate(int cp) {
        return cp >= 0xD800 && cp <= 0xDFFF;
    }
}
