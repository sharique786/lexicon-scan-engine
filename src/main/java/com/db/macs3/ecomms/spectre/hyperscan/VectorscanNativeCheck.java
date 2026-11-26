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
 * Refuses to run on the Linux x86_64 Vectorscan 5.4.11 / 5.4.12 native libraries, which crash the JVM.
 *
 * <h2>The bug (root cause of the executor {@code SIGSEGV ... C [libhs_runtime.so+0x4a6c91]})</h2>
 * Vectorscan 5.4.11 and 5.4.12 (hyperscan-java 5.4.11-3.x, natives {@code 5.4.11-2.0.x}/{@code 5.4.12-2.0.4}) shipped
 * leftover debug code in {@code rvermicelliDoubleExecReal} ({@code src/nfa/vermicelli_simd.cpp}):
 * {@code char s[255]; snprintf(s, buf_end - buf + 1, "%s", buf);}. Any scan that runs this reverse double-byte
 * search over more than ~255 bytes of text overflows the stack buffer with message text, and the function's
 * {@code ret} jumps to "8 bytes of text": the JVM dies. It depends only on the lexicon patterns and the text.
 *
 * <p>Fixed upstream in Vectorscan 5.4.13 (VectorCamp/vectorscan#392, commit {@code fe94004}), which hyperscan-java
 * {@code 5.4.13-3.1.1} (natives {@code 5.4.13-2.0.5}) bundles — the version the {@code hyperscan-latest} profile uses.
 * Its libraries have exactly four fewer {@code call snprintf@plt} sites than 5.4.11/5.4.12 (one per CPU variant of the
 * fat runtime). Until v3.34 this project shipped binary-patched 5.4.11 copies instead.
 *
 * <p>This check hashes the libraries actually on the classpath once per JVM and fails fast with a clear message if a
 * known crashing build would be loaded — e.g. someone forcing {@code -Dhyperscan.version=5.4.11-3.1.0}. Other
 * platforms (no such resource) are skipped; an unknown hash is only logged.
 */
public final class VectorscanNativeCheck {

    private static final Logger log = LoggerFactory.getLogger(VectorscanNativeCheck.class);

    static final String RESOURCE_DIR = "/com/gliwka/hyperscan/jni/linux-x86_64/";

    static final String RUNTIME = "libhs_runtime.so";
    static final String COMPILER = "libhs.so";

    /** sha256 -> build, of the crashing linux-x86_64 libraries (native jars 5.4.11-2.0.2 and 5.4.12-2.0.4). */
    static final Map<String, String> BUGGY = Map.of(
            "29c340449f77fef4359e6a5b121f81fcaa3dd81a3e7bb4ee14ebb5908b24c5a7", "5.4.11-2.0.2 " + RUNTIME,
            "4f7763cdc55c884da1aa4d822ddf3eab4e17c0e990bd760f9a8268b5165f80f2", "5.4.11-2.0.2 " + COMPILER,
            "7f839f7b0749e20861abf11d0b680990e656c821278df256a85c3f9bf4608a28", "5.4.12-2.0.4 " + RUNTIME,
            "5736eb1f3c056a86d0185f8d9d92bb419f8ea702410dd97b075590e480914ccf", "5.4.12-2.0.4 " + COMPILER);

    /** sha256 -> build, of the fixed linux-x86_64 libraries in native-5.4.13-2.0.5-linux-x86_64.jar. */
    static final Map<String, String> FIXED = Map.of(
            "42b5dbfe71505e34086a58dfdfbe7a361ae0ec7e9920a7ea5ab2838ca44d0416", "5.4.13-2.0.5 " + RUNTIME,
            "c7f88f836c8912faee827436ca16814992fcd4e8d1bae9ea59b4481bccce1d17", "5.4.13-2.0.5 " + COMPILER);

    private static volatile boolean verified;

    private VectorscanNativeCheck() {
    }

    /** Thrown when a crashing native build would be loaded. */
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
        for (String lib : new String[]{RUNTIME, COMPILER}) {
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
            String buggy = BUGGY.get(hash);
            if (buggy != null) {
                throw new UnpatchedVectorscanException("The classpath carries Vectorscan " + buggy + " (sha256 "
                        + hash + "), whose rvermicelliDoubleExec overflows its stack and kills the JVM (SIGSEGV in "
                        + "libhs_runtime.so). Use hyperscan-java 5.4.13-3.1.1 or later (the hyperscan-latest "
                        + "profile) — see VectorscanNativeCheck.");
            }
            String fixed = FIXED.get(hash);
            if (fixed != null) {
                log.info("Vectorscan {}: fixed build verified (sha256 {})", fixed, hash);
            } else {
                log.warn("Vectorscan {}: unrecognised build (sha256 {}) — not a known crashing 5.4.11/5.4.12 build, "
                        + "but not verified as 5.4.13 either", lib, hash);
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
