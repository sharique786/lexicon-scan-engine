package com.db.macs3.ecomms.spectre.reader;

import com.db.macs3.ecomms.spectre.model.MessageRecord;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import java.util.Set;

/**
 * Common contract for reading communication messages into a
 * {@code Dataset<MessageRecord>}, regardless of the underlying file format.
 *
 * <h2>Why an interface</h2>
 * <p>Production runs read AVRO files from GCS ({@link AvroMessageReader}).
 * Per the testing requirement that integration tests must run identically on
 * Windows, macOS, Linux, and GitHub Actions CI — without needing a live GCS
 * bucket or AVRO tooling — a JSON-based reader ({@link JsonMessageReader})
 * implements the same contract, reading fixture files from the local/test
 * classpath or filesystem instead. {@link com.db.macs3.ecomms.spectre.engine.LexiconScanEngine}
 * depends only on this interface, so swapping readers requires no change to
 * scanning logic — only which {@code MessageReader} bean is wired in for a
 * given Spring profile.
 */
public interface MessageReader {

    /**
     * Reads messages and filters to only those whose {@code message_id}
     * appears in {@code broadcastMessageIds}.
     *
     * @param spark               active Spark session
     * @param sourcePath          format-specific source location (GCS path for AVRO,
     *                            local/classpath directory for JSON)
     * @param broadcastMessageIds broadcast set of message IDs to include
     * @return dataset of {@link MessageRecord}s, one per message
     */
    Dataset<MessageRecord> readAndFilter(SparkSession spark, String sourcePath,
                                          Broadcast<Set<String>> broadcastMessageIds);
}
