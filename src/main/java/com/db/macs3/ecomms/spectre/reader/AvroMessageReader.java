package com.db.macs3.ecomms.spectre.reader;

import com.db.macs3.ecomms.spectre.model.MessageRecord;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.apache.spark.sql.functions.*;

/**
 * Production {@link MessageReader}: reads AVRO message files from a GCS path.
 *
 * <h2>AVRO structure handled</h2>
 * <pre>
 * {
 *   "message_id": string, "source_type": "chat"|"email", "run_date": string,
 *   "metadata": {...},
 *   "content": { "raw_text": string, "subject": string, "clean_text": string },
 *   "attachment": [ { "raw_text": string, "content_type": string, "content_encoding": string } ]
 * }
 * </pre>
 *
 * <h2>Optimisations</h2>
 * <ul>
 *   <li>Projection pushdown — only {@code message_id}, {@code source_type},
 *       {@code run_date}, {@code content.raw_text}, {@code attachment[].raw_text}
 *       are selected; other fields never leave disk.</li>
 *   <li>Broadcast-based filtering to relevant message IDs — avoids a shuffle join.</li>
 *   <li>Attachment texts collected via {@code transform()} (a native Spark
 *       higher-order function, not a UDF) to avoid a costly explode-then-collect.</li>
 * </ul>
 */
@Component
public class AvroMessageReader implements MessageReader {

    private static final Logger log = LoggerFactory.getLogger(AvroMessageReader.class);

    @Override
    public Dataset<MessageRecord> readAndFilter(SparkSession spark, String avroPath,
                                                 Broadcast<Set<String>> broadcastMessageIds) {
        log.info("Reading AVRO messages from: {}", avroPath);

        Dataset<Row> rawDf = spark.read().format("avro").load(avroPath);
        log.debug("AVRO raw schema: {}", rawDf.schema().treeString());

        Dataset<Row> projected = rawDf.select(
                col("message_id"), col("source_type"), col("run_date"),
                col("content.raw_text").alias("content_raw_text"),
                col("attachment")
        );

        Dataset<Row> withAttachments = projected.withColumn(
                "attachment_texts", expr("transform(attachment, a -> a.raw_text)")
        ).drop("attachment");

        Dataset<Row> filtered = withAttachments.filter(
                (FilterFunction<Row>) row -> {
                    int idx = row.fieldIndex("message_id");
                    String msgId = row.isNullAt(idx) ? null : row.getString(idx);
                    return msgId != null && broadcastMessageIds.value().contains(msgId);
                }
        );

        return filtered.map(
                (MapFunction<Row, MessageRecord>) AvroMessageReader::rowToMessageRecord,
                Encoders.bean(MessageRecord.class)
        );
    }

    @SuppressWarnings("unchecked")
    private static MessageRecord rowToMessageRecord(Row row) {
        String messageId      = getStrOrNull(row, "message_id");
        String sourceType     = getStrOrNull(row, "source_type");
        String runDate        = getStrOrNull(row, "run_date");
        String contentRawText = getStrOrNull(row, "content_raw_text");

        List<String> attachmentTexts = new ArrayList<>();
        int attIdx = row.fieldIndex("attachment_texts");
        if (!row.isNullAt(attIdx)) {
            scala.collection.Seq<Object> attSeq = row.getSeq(attIdx);
            if (attSeq != null) {
                scala.collection.Iterator<Object> it = attSeq.iterator();
                while (it.hasNext()) {
                    Object item = it.next();
                    if (item != null) {
                        String txt = item.toString();
                        if (!txt.isBlank()) attachmentTexts.add(txt);
                    }
                }
            }
        }
        return MessageRecord.of(messageId, sourceType, runDate, contentRawText, attachmentTexts);
    }

    private static String getStrOrNull(Row row, String fieldName) {
        int idx = row.fieldIndex(fieldName);
        return row.isNullAt(idx) ? null : row.getString(idx);
    }
}
