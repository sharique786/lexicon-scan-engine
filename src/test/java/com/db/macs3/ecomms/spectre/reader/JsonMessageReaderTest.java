package com.db.macs3.ecomms.spectre.reader;

import com.db.macs3.ecomms.spectre.model.MessageRecord;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link JsonMessageReader}'s file-loading logic
 * ({@code loadAll}), independent of Spark. Uses {@link TempDir} (JUnit 5) so
 * every path is created fresh per test using {@link java.nio.file.Path} APIs
 * — no hard-coded {@code '/'} or {@code '\\'} separators anywhere, which is
 * what makes this test pass identically on Windows, macOS, and Linux.
 */
@DisplayName("JsonMessageReader Tests")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class JsonMessageReaderTest {

    private JsonMessageReader reader;

    @BeforeEach
    void setUp() {
        reader = new JsonMessageReader();
    }

    // ── Classpath loading ────────────────────────────────────────────────────

    @Test @Order(1)
    @DisplayName("Loads messages from a classpath: resource (the real scenario1.json fixture)")
    void loadsFromClasspathResource() throws IOException {
        List<MessageRecord> messages = reader.loadAll("classpath:fixtures/messages/scenario1.json");
        assertThat(messages).isNotEmpty();
        assertThat(messages.stream().map(MessageRecord::getMessageId))
                .contains("msg-201", "msg-202", "msg-203", "msg-204");
    }

    @Test @Order(2)
    @DisplayName("Classpath resource with backslash path separator is normalised")
    void classpathBackslashNormalised() throws IOException {
        // Windows-style path input must still resolve correctly via classloader lookup
        List<MessageRecord> messages = reader.loadAll("classpath:fixtures\\messages\\scenario1.json");
        assertThat(messages).isNotEmpty();
    }

    @Test @Order(3)
    @DisplayName("Missing classpath resource throws IOException")
    void missingClasspathResource_throws() {
        assertThatIOException(() -> reader.loadAll("classpath:fixtures/does-not-exist.json"));
    }

    // ── Single-file loading (java.nio.file.Path — OS-agnostic) ─────────────────

    @Test @Order(10)
    @DisplayName("Loads messages from a single JSON array file on disk")
    void loadsFromSingleFile(@TempDir Path tempDir) throws IOException {
        Path file = tempDir.resolve("messages.json");
        Files.writeString(file, "[" +
                "{\"message_id\":\"m1\",\"source_type\":\"chat\",\"run_date\":\"20260101\"," +
                "\"content\":{\"raw_text\":\"hello world\"},\"attachment\":[]}," +
                "{\"message_id\":\"m2\",\"source_type\":\"email\",\"run_date\":\"20260101\"," +
                "\"content\":{\"raw_text\":\"another message\"},\"attachment\":[]}" +
                "]", StandardCharsets.UTF_8);

        List<MessageRecord> messages = reader.loadAll(file.toString());
        assertThat(messages).hasSize(2);
        assertThat(messages.get(0).getMessageId()).isEqualTo("m1");
        assertThat(messages.get(0).getContentRawText()).isEqualTo("hello world");
    }

    @Test @Order(11)
    @DisplayName("Loads a single JSON OBJECT (not wrapped in an array) as one message")
    void loadsSingleObjectNotArray(@TempDir Path tempDir) throws IOException {
        Path file = tempDir.resolve("single.json");
        Files.writeString(file,
                "{\"message_id\":\"solo\",\"source_type\":\"chat\",\"run_date\":\"20260101\"," +
                "\"content\":{\"raw_text\":\"solo message\"}}", StandardCharsets.UTF_8);

        List<MessageRecord> messages = reader.loadAll(file.toString());
        assertThat(messages).hasSize(1);
        assertThat(messages.get(0).getMessageId()).isEqualTo("solo");
    }

    @Test @Order(12)
    @DisplayName("Missing file path throws IOException")
    void missingFile_throws(@TempDir Path tempDir) {
        Path missing = tempDir.resolve("does-not-exist.json");
        assertThatIOException(() -> reader.loadAll(missing.toString()));
    }

    // ── Directory loading — multiple files merged, deterministic order ────────

    @Test @Order(20)
    @DisplayName("Loads and merges messages from ALL *.json files in a directory")
    void loadsFromDirectory_mergesAllFiles(@TempDir Path tempDir) throws IOException {
        Files.writeString(tempDir.resolve("a.json"),
                "[{\"message_id\":\"a1\",\"content\":{\"raw_text\":\"from file a\"}}]", StandardCharsets.UTF_8);
        Files.writeString(tempDir.resolve("b.json"),
                "[{\"message_id\":\"b1\",\"content\":{\"raw_text\":\"from file b\"}}]", StandardCharsets.UTF_8);
        // Non-JSON file must be ignored
        Files.writeString(tempDir.resolve("readme.txt"), "not json", StandardCharsets.UTF_8);

        List<MessageRecord> messages = reader.loadAll(tempDir.toString());
        assertThat(messages).hasSize(2);
        assertThat(messages.stream().map(MessageRecord::getMessageId)).containsExactlyInAnyOrder("a1", "b1");
    }

    @Test @Order(21)
    @DisplayName("Directory with nested subdirectories: recursively finds JSON files")
    void loadsFromDirectory_recursive(@TempDir Path tempDir) throws IOException {
        Path subDir = tempDir.resolve("nested");
        Files.createDirectory(subDir);
        Files.writeString(subDir.resolve("nested.json"),
                "[{\"message_id\":\"n1\",\"content\":{\"raw_text\":\"nested message\"}}]", StandardCharsets.UTF_8);

        List<MessageRecord> messages = reader.loadAll(tempDir.toString());
        assertThat(messages.stream().map(MessageRecord::getMessageId)).contains("n1");
    }

    // ── Attachment parsing ────────────────────────────────────────────────────

    @Test @Order(30)
    @DisplayName("Parses multiple attachments from an ARRAY, preserving order")
    void parsesAttachmentArray(@TempDir Path tempDir) throws IOException {
        Path file = tempDir.resolve("with-attachments.json");
        Files.writeString(file,
                "{\"message_id\":\"att1\",\"content\":{\"raw_text\":\"body text\"}," +
                "\"attachment\":[" +
                "{\"raw_text\":\"first attachment\"}," +
                "{\"raw_text\":\"second attachment\"}" +
                "]}", StandardCharsets.UTF_8);

        List<MessageRecord> messages = reader.loadAll(file.toString());
        assertThat(messages).hasSize(1);
        assertThat(messages.get(0).getAttachmentTexts())
                .containsExactly("first attachment", "second attachment");
    }

    @Test @Order(31)
    @DisplayName("Tolerates a single attachment OBJECT not wrapped in an array")
    void toleratesSingleAttachmentObject(@TempDir Path tempDir) throws IOException {
        Path file = tempDir.resolve("single-attachment.json");
        Files.writeString(file,
                "{\"message_id\":\"att2\",\"content\":{\"raw_text\":\"body\"}," +
                "\"attachment\":{\"raw_text\":\"lone attachment\"}}", StandardCharsets.UTF_8);

        List<MessageRecord> messages = reader.loadAll(file.toString());
        assertThat(messages.get(0).getAttachmentTexts()).containsExactly("lone attachment");
    }

    @Test @Order(32)
    @DisplayName("Blank attachment raw_text values are excluded")
    void blankAttachmentTextExcluded(@TempDir Path tempDir) throws IOException {
        Path file = tempDir.resolve("blank-attachment.json");
        Files.writeString(file,
                "{\"message_id\":\"att3\",\"content\":{\"raw_text\":\"body\"}," +
                "\"attachment\":[{\"raw_text\":\"\"},{\"raw_text\":\"real content\"}]}", StandardCharsets.UTF_8);

        List<MessageRecord> messages = reader.loadAll(file.toString());
        assertThat(messages.get(0).getAttachmentTexts()).containsExactly("real content");
    }

    // ── Helper ────────────────────────────────────────────────────────────────

    private interface ThrowingRunnable {
        void run() throws IOException;
    }

    private void assertThatIOException(ThrowingRunnable runnable) {
        try {
            runnable.run();
            org.junit.jupiter.api.Assertions.fail("Expected IOException was not thrown");
        } catch (IOException expected) {
            // expected
        }
    }
}
