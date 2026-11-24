package com.db.macs3.ecomms.spectre.hyperscan;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Map;
import java.util.function.Function;

/**
 * Refuses to run on the UNPATCHED Linux Vectorscan 5.4.11 native libraries, which crash the JVM.
 *
 * <h2>The bug (root cause of the executor {@code SIGSEGV ... C [libhs_runtime.so+0x4a6c91]})</h2>
 * {@code com.gliwka.hyperscan:native:5.4.11-2.0.2:linux-x86_64} (pulled in by hyperscan-java 5.4.11-3.1.0, the
 * newest release) ships Vectorscan with leftover debug code in {@code rvermicelliDoubleExecReal}:
 * {@code char s[255]; snprintf(s, buf_end - buf + 1, "%s", buf);}. Any scan that runs this reverse double-byte
 * search over more than ~255 bytes of text overflows the stack buffer with message text, and the function's
 * {@code ret} jumps to "8 bytes of text": the JVM dies. It depends only on the lexicon patterns and the text, not on
 * caching or threading — more lexicons per message / more messages simply make it likelier. Reproduced on Linux with
 * the stock hyperscan-java {@code Scanner} (same crash offset); see {@code tools/vectorscan-patch/}.
 *
 * <p>The build ships patched copies (each such {@code call snprintf} replaced by a NOP — its result was never used)
 * from {@code src/main/native-patched}, which shadow the jar's originals on the classpath and in the shaded jar. This
 * check hashes the libraries actually on the classpath once per JVM and fails fast with a clear message if the
 * original, crashing build is the one that would be loaded — e.g. a jar built without the {@code hyperscan-latest}
 * profile's resources. Other platforms/versions (no such resource, or an unknown hash) are only logged.
 */
public final class VectorscanNativeCheck {

    private static final Logger log = LoggerFactory.getLogger(VectorscanNativeCheck.class);

    static final String RESOURCE_DIR = "/com/gliwka/hyperscan/jni/linux-x86_64/";

    /** sha256 of the ORIGINAL (crashing) libraries in native-5.4.11-2.0.2-linux-x86_64.jar. */
    static final Map<String, String> BUGGY = Map.of(
            "libhs_runtime.so", "29c340449f77fef4359e6a5b121f81fcaa3dd81a3e7bb4ee14ebb5908b24c5a7",
            "libhs.so", "4f7763cdc55c884da1aa4d822ddf3eab4e17c0e990bd760f9a8268b5165f80f2");

    /** sha256 of the patched libraries produced by tools/vectorscan-patch/patch_vectorscan.py. */
    static final Map<String, String> PATCHED = Map.of(
            "libhs_runtime.so", "fa1d1179fc4396514bdeedbdff9b413d299c24984a67fb7e3e3f4f29fae66541",
            "libhs.so", "077b4e00eed6782260c95f26b1c714d368b3d65c397b5cbd9755dbae8de3b19e");

    private static volatile boolean verified;

    private VectorscanNativeCheck() {
    }

    /** Thrown when the crashing native build would be loaded. */
    public static final class UnpatchedVectorscanException extends IllegalStateException {
        public UnpatchedVectorscanException(String message) {
            super(message);
        }
    }

    /** Checks once per JVM (cheap afterwards). Call before any Hyperscan scan on driver and executors. */
    public static void verifyOnce() {
        if (verified) {
            return;
        }
        synchronized (VectorscanNativeCheck.class) {
            if (!verified) {
                verify(name -> VectorscanNativeCheck.class.getResourceAsStream(RESOURCE_DIR + name));
                verified = true;
            }
        }
    }

    /** @param open opens a library by file name, or returns null when the classpath has none */
    static void verify(Function<String, InputStream> open) {
        for (String lib : BUGGY.keySet()) {
            String hash;
            try (InputStream in = open.apply(lib)) {
                if (in == null) {
                    log.debug("No Linux x86_64 {} on the classpath — Vectorscan check not applicable", lib);
                    continue;
                }
                hash = sha256(in);
            } catch (IOException e) {
                throw new IllegalStateException("Could not read " + RESOURCE_DIR + lib + ": " + e.getMessage(), e);
            }
            if (BUGGY.get(lib).equals(hash)) {
                throw new UnpatchedVectorscanException("The classpath carries the ORIGINAL Vectorscan 5.4.11 " + lib
                        + " (sha256 " + hash + "), whose rvermicelliDoubleExec overflows its stack and kills the JVM "
                        + "(SIGSEGV in libhs_runtime.so). Build the jar with the hyperscan-latest profile so the "
                        + "patched copies in src/main/native-patched are packaged — see VectorscanNativeCheck.");
            }
            if (PATCHED.get(lib).equals(hash)) {
                log.info("Vectorscan {}: patched build verified (sha256 {})", lib, hash);
            } else {
                log.warn("Vectorscan {}: unrecognised build (sha256 {}) — not the known crashing 5.4.11 build, "
                        + "but not verified as patched either", lib, hash);
            }
        }
    }

    static String sha256(InputStream in) throws IOException {
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
        byte[] buffer = new byte[1 << 16];
        for (int n; (n = in.read(buffer)) > 0; ) {
            digest.update(buffer, 0, n);
        }
        return HexFormat.of().formatHex(digest.digest());
    }
}
