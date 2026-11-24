package com.db.macs3.ecomms.spectre.hyperscan;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@DisplayName("VectorscanNativeCheck — never run on the crashing Vectorscan 5.4.11 natives")
class VectorscanNativeCheckTest {

    private static final Path PATCHED_DIR = Path.of("src/main/native-patched/com/gliwka/hyperscan/jni/linux-x86_64");

    /** Patched call sites (file offset of each NOPed `call snprintf@plt`), per tools/vectorscan-patch. */
    private static final Map<String, long[]> SITES = Map.of(
            "libhs_runtime.so", new long[]{0x118349, 0x225619, 0x33E8DE, 0x4A6A4C},
            "libhs.so", new long[]{0x47E389, 0x58B659, 0x6A491E, 0x80CA8C});

    private static InputStream open(Path file) {
        try {
            return Files.newInputStream(file);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Test
    @DisplayName("the committed patched libraries hash to the values the check accepts")
    void committedLibrariesArePatched() throws IOException {
        for (String lib : VectorscanNativeCheck.PATCHED.keySet()) {
            try (InputStream in = open(PATCHED_DIR.resolve(lib))) {
                assertThat(VectorscanNativeCheck.sha256(in)).as(lib).isEqualTo(VectorscanNativeCheck.PATCHED.get(lib));
            }
        }
    }

    @Test
    @DisplayName("patched libraries accepted; libraries absent (Windows/macOS) accepted")
    void acceptsPatchedOrAbsent() {
        VectorscanNativeCheck.verify(name -> open(PATCHED_DIR.resolve(name)));
        VectorscanNativeCheck.verify(name -> null);
    }

    @Test
    @DisplayName("the original crashing build is rejected with a clear exception, not loaded")
    void rejectsOriginalBuild() throws IOException {
        Path original = originalNativeJar();
        assumeTrue(original != null, "native-5.4.11-2.0.2-linux-x86_64.jar not in the local Maven repository");
        Map<String, byte[]> libs = readLibs(original);

        assertThatThrownBy(() -> VectorscanNativeCheck.verify(name -> new ByteArrayInputStream(libs.get(name))))
                .isInstanceOf(VectorscanNativeCheck.UnpatchedVectorscanException.class)
                .hasMessageContaining("rvermicelliDoubleExec");
    }

    @Test
    @DisplayName("patched = original except 4 five-byte NOPs per library, each replacing a call")
    void patchChangesOnlyTheSnprintfCalls() throws IOException {
        Path original = originalNativeJar();
        assumeTrue(original != null, "native-5.4.11-2.0.2-linux-x86_64.jar not in the local Maven repository");
        Map<String, byte[]> originals = readLibs(original);

        for (Map.Entry<String, long[]> lib : SITES.entrySet()) {
            byte[] before = originals.get(lib.getKey());
            byte[] after = Files.readAllBytes(PATCHED_DIR.resolve(lib.getKey()));
            assertThat(after).hasSameSizeAs(before);
            int changed = 0;
            for (int i = 0; i < before.length; i++) {
                if (before[i] != after[i]) {
                    changed++;
                }
            }
            assertThat(changed).as(lib.getKey() + " changed bytes").isLessThanOrEqualTo(20);
            for (long site : lib.getValue()) {
                int s = (int) site;
                assertThat(before[s] & 0xFF).as("call opcode at %x", site).isEqualTo(0xE8);
                assertThat(new byte[]{after[s], after[s + 1], after[s + 2], after[s + 3], after[s + 4]})
                        .as("5-byte NOP at %x", site).containsExactly(0x0F, 0x1F, 0x44, 0x00, 0x00);
            }
        }
    }

    private static Path originalNativeJar() {
        Path jar = Path.of(System.getProperty("user.home"), ".m2", "repository", "com", "gliwka", "hyperscan",
                "native", "5.4.11-2.0.2", "native-5.4.11-2.0.2-linux-x86_64.jar");
        return Files.isRegularFile(jar) ? jar : null;
    }

    private static Map<String, byte[]> readLibs(Path jar) throws IOException {
        Map<String, byte[]> libs = new HashMap<>();
        try (ZipFile zip = new ZipFile(jar.toFile())) {
            for (String lib : SITES.keySet()) {
                ZipEntry entry = zip.getEntry(VectorscanNativeCheck.RESOURCE_DIR.substring(1) + lib);
                try (InputStream in = zip.getInputStream(entry)) {
                    libs.put(lib, in.readAllBytes());
                }
            }
        }
        return libs;
    }
}
