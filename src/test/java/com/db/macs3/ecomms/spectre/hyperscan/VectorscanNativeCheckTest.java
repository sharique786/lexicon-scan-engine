package com.db.macs3.ecomms.spectre.hyperscan;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@DisplayName("VectorscanNativeCheck — never run on the crashing Vectorscan 5.4.11/5.4.12 natives")
class VectorscanNativeCheckTest {

    private static Map<String, byte[]> libsOf(String nativeVersion) throws IOException {
        Path jar = Path.of(System.getProperty("user.home"), ".m2", "repository", "com", "gliwka", "hyperscan",
                "native", nativeVersion, "native-" + nativeVersion + "-linux-x86_64.jar");
        assumeTrue(Files.isRegularFile(jar), jar.getFileName() + " not in the local Maven repository");
        Map<String, byte[]> libs = new HashMap<>();
        try (ZipFile zip = new ZipFile(jar.toFile())) {
            for (String lib : new String[]{VectorscanNativeCheck.RUNTIME, VectorscanNativeCheck.COMPILER}) {
                ZipEntry entry = zip.getEntry(VectorscanNativeCheck.RESOURCE_DIR.substring(1) + lib);
                try (InputStream in = zip.getInputStream(entry)) {
                    libs.put(lib, in.readAllBytes());
                }
            }
        }
        return libs;
    }

    @Test
    @DisplayName("libraries absent (Windows/macOS) are accepted")
    void acceptsAbsent() {
        VectorscanNativeCheck.verify(name -> null);
    }

    @Test
    @DisplayName("unknown libraries are accepted (logged only)")
    void acceptsUnknown() {
        VectorscanNativeCheck.verify(name -> new ByteArrayInputStream(new byte[]{1, 2, 3}));
    }

    @Test
    @DisplayName("the fixed 5.4.13 natives hash to the values the check knows, and are accepted")
    void acceptsFixedBuild() throws IOException {
        Map<String, byte[]> libs = libsOf("5.4.13-2.0.5");
        for (Map.Entry<String, byte[]> lib : libs.entrySet()) {
            String hash = VectorscanNativeCheck.sha256(new ByteArrayInputStream(lib.getValue()));
            assertThat(VectorscanNativeCheck.FIXED).as(lib.getKey()).containsKey(hash);
        }
        VectorscanNativeCheck.verify(name -> new ByteArrayInputStream(libs.get(name)));
    }

    @ParameterizedTest
    @ValueSource(strings = {"5.4.11-2.0.2", "5.4.12-2.0.4"})
    @DisplayName("the crashing builds are rejected with a clear exception, not loaded")
    void rejectsBuggyBuild(String nativeVersion) throws IOException {
        Map<String, byte[]> libs = libsOf(nativeVersion);
        assertThatThrownBy(() -> VectorscanNativeCheck.verify(name -> new ByteArrayInputStream(libs.get(name))))
                .isInstanceOf(VectorscanNativeCheck.UnpatchedVectorscanException.class)
                .hasMessageContaining("rvermicelliDoubleExec")
                .hasMessageContaining(nativeVersion);
    }
}
