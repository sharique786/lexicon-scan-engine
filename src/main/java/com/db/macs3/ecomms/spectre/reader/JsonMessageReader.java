package com.db.macs3.ecomms.spectre.reader;

import com.db.macs3.ecomms.spectre.model.MessageRecord;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Integration-test {@link MessageReader}: reads communication messages from
 * local JSON fixture files rather than GCS/AVRO.
 *
 * <h2>Why this exists</h2>
 * <p>Per the requirement that integration tests must run identically on
 * Windows, macOS, Linux, and GitHub Actions CI runners — without needing live
 * GCP credentials, a GCS bucket, or AVRO tooling installed — this reader
 * loads the same logical message structure from plain JSON files using only
 * {@link java.nio.file.Path} APIs (which are OS-path-separator-agnostic) and
 * Jackson. No cloud SDK calls are made.
 *
 * <h2>Supported source forms</h2>
 * <p>{@code sourcePath} may be:
 * <ul>
 *   <li>A path to a single JSON file containing a JSON ARRAY of message objects</li>
 *   <li>A path to a directory containing one or more {@code *.json} files, each
 *       either a single message object OR a JSON array of message objects
 *       (all are merged together)</li>
 *   <li>A classpath resource, prefixed with {@code classpath:} (e.g.
 *       {@code classpath:fixtures/messages/scenario1.json}) — resolved via
 *       the current thread's context classloader, working identically
 *       whether tests run from an IDE, {@code mvn test}, or a packaged JAR</li>
 * </ul>
 *
 * <h2>JSON message shape (mirrors the production AVRO schema)</h2>
 * <pre>
 * {
 *   "message_id": "msg-101",
 *   "source_type": "chat",
 *   "run_date": "20260713",
 *   "content": { "raw_text": "...", "subject": "...", "clean_text": "..." },
 *   "attachment": [ { "raw_text": "...", "content_type": "text/plain" } ]
 * }
 * </pre>
 */
@Component
public class JsonMessageReader implements MessageReader {

    private static final Logger log = LoggerFactory.getLogger(JsonMessageReader.class);
    private static final String CLASSPATH_PREFIX = "classpath:";

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public Dataset<MessageRecord> readAndFilter(SparkSession spark, String sourcePath,
                                                 Broadcast<Set<String>> broadcastMessageIds) {
        log.info("Reading JSON messages from: {}", sourcePath);
        List<MessageRecord> all;
        try {
            all = loadAll(sourcePath);
        } catch (IOException e) {
            throw new RuntimeException("Failed to read JSON message fixtures from " + sourcePath, e);
        }

        Set<String> allowedIds = broadcastMessageIds.value();
        List<MessageRecord> filtered = new ArrayList<>();
        for (MessageRecord m : all) {
            if (m.getMessageId() != null && allowedIds.contains(m.getMessageId())) {
                filtered.add(m);
            }
        }
        log.info("JSON messages loaded: {} total, {} matched message_id filter", all.size(), filtered.size());

        return spark.createDataset(filtered, Encoders.bean(MessageRecord.class));
    }

    // ── Loading ───────────────────────────────────────────────────────────────

    /**
     * Loads every message from {@code sourcePath}, handling all three
     * supported source forms (classpath resource, single file, directory).
     *
     * @param sourcePath the source location (see class Javadoc for supported forms)
     * @return every message found, unfiltered
     */
    List<MessageRecord> loadAll(String sourcePath) throws IOException {
        if (sourcePath.startsWith(CLASSPATH_PREFIX)) {
            return loadFromClasspath(sourcePath.substring(CLASSPATH_PREFIX.length()));
        }

        Path path = Paths.get(sourcePath);
        if (!Files.exists(path)) {
            throw new IOException("JSON message source not found: " + sourcePath);
        }
        if (Files.isDirectory(path)) {
            return loadFromDirectory(path);
        }
        return loadFromSingleFile(path);
    }

    private List<MessageRecord> loadFromClasspath(String resourcePath) throws IOException {
        // Normalise Windows-style backslashes if a caller accidentally supplies them —
        // classloader resource paths are always '/'-separated regardless of host OS.
        String normalised = resourcePath.replace('\\', '/');
        try (InputStream is = Thread.currentThread().getContextClassLoader()
                                     .getResourceAsStream(normalised)) {
            if (is == null) {
                throw new IOException("Classpath resource not found: " + normalised);
            }
            JsonNode root = objectMapper.readTree(is);
            return parseNode(root);
        }
    }

    private List<MessageRecord> loadFromSingleFile(Path file) throws IOException {
        try (InputStream is = Files.newInputStream(file)) {
            JsonNode root = objectMapper.readTree(is);
            return parseNode(root);
        }
    }

    private List<MessageRecord> loadFromDirectory(Path dir) throws IOException {
        List<MessageRecord> all = new ArrayList<>();
        try (Stream<Path> paths = Files.walk(dir)) {
            List<Path> jsonFiles = paths
                    .filter(Files::isRegularFile)
                    .filter(p -> p.toString().toLowerCase().endsWith(".json"))
                    .sorted(Comparator.naturalOrder()) // deterministic order across OSes
                    .collect(java.util.stream.Collectors.toList());

            for (Path jsonFile : jsonFiles) {
                all.addAll(loadFromSingleFile(jsonFile));
            }
        }
        return all;
    }

    /**
     * Parses a JsonNode that is either a single message object or a JSON
     * array of message objects into a list of {@link MessageRecord}.
     */
    private List<MessageRecord> parseNode(JsonNode root) {
        List<MessageRecord> result = new ArrayList<>();
        if (root.isArray()) {
            for (JsonNode msgNode : root) {
                result.add(parseMessage(msgNode));
            }
        } else if (root.isObject()) {
            result.add(parseMessage(root));
        }
        return result;
    }

    private MessageRecord parseMessage(JsonNode node) {
        String messageId  = textOrNull(node, "message_id");
        String sourceType = textOrNull(node, "source_type");
        String runDate    = textOrNull(node, "run_date");

        JsonNode contentNode = node.get("content");
        String contentRawText = contentNode != null ? textOrNull(contentNode, "raw_text") : null;

        List<String> attachmentTexts = new ArrayList<>();
        JsonNode attachmentNode = node.get("attachment");
        if (attachmentNode != null && attachmentNode.isArray()) {
            for (JsonNode att : attachmentNode) {
                String rawText = textOrNull(att, "raw_text");
                if (rawText != null && !rawText.isBlank()) {
                    attachmentTexts.add(rawText);
                }
            }
        } else if (attachmentNode != null && attachmentNode.isObject()) {
            // Tolerate a single attachment object (not wrapped in an array) —
            // matches the sample AVRO schema shown in the original requirements doc.
            String rawText = textOrNull(attachmentNode, "raw_text");
            if (rawText != null && !rawText.isBlank()) {
                attachmentTexts.add(rawText);
            }
        }

        return MessageRecord.of(messageId, sourceType, runDate, contentRawText, attachmentTexts);
    }

    private static String textOrNull(JsonNode node, String field) {
        JsonNode v = node.get(field);
        return (v == null || v.isNull()) ? null : v.asText();
    }
}
