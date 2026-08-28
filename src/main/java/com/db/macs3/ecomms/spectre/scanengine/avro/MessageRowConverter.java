package com.db.macs3.ecomms.spectre.scanengine.avro;

import com.db.macs3.ecomms.spectre.scanengine.model.message.MessageAttachment;
import com.db.macs3.ecomms.spectre.scanengine.model.message.MessageContent;
import com.db.macs3.ecomms.spectre.scanengine.model.message.MessageProcessing;
import com.db.macs3.ecomms.spectre.scanengine.model.message.MessageSource;
import com.db.macs3.ecomms.spectre.scanengine.model.message.ScanMessage;
import org.apache.spark.sql.Row;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Converts one Spark {@link Row} of AVRO message data into a
 * {@link ScanMessage} — the one place that knows the AVRO schema's nested
 * shape ({@code source.*}, {@code message.content.*}, the
 * {@code attachments} array's {@code metadata}/{@code content} sub-structs,
 * {@code processing.*}), so every other class works with {@link ScanMessage}
 * directly. See {@link ScanMessage} class Javadoc for the confirmed AVRO
 * schema this reads against.
 *
 * <p>{@link #datasetId}/{@link #restricted} are NOT present in the AVRO
 * itself — they are supplied by the caller ({@code MessageAvroReader}, which
 * knows which dataset/subfolder a given file was read from), not read from
 * {@code row}.
 *
 * <p>Not independently executable-verified in this project's development
 * sandbox — see {@code GcsClient} class Javadoc.
 */
public final class MessageRowConverter implements Serializable {

    private MessageRowConverter() {}

    public static ScanMessage fromRow(Row row, String datasetId, boolean restricted) {
        String messageId = getStringOrNull(row, "message_id");

        MessageSource source = null;
        if (hasNonNullField(row, "source")) {
            Row sourceRow = row.getAs("source");
            source = new MessageSource(
                    getStringOrNull(sourceRow, "channel_name"),
                    getStringOrNull(sourceRow, "source_name"),
                    getStringOrNull(sourceRow, "src_sys_name"),
                    getStringOrNull(sourceRow, "src_sys_conv_id"));
        }

        MessageContent content = null;
        if (hasNonNullField(row, "message")) {
            Row messageRow = row.getAs("message");
            if (hasNonNullField(messageRow, "content")) {
                Row contentRow = messageRow.getAs("content");
                content = new MessageContent(
                        getStringOrNull(contentRow, "header"),
                        getStringOrNull(contentRow, "raw_text"),
                        getStringOrNull(contentRow, "subject"),
                        getStringOrNull(contentRow, "clean_text"));
            }
        }

        List<MessageAttachment> attachments = new ArrayList<>();
        if (hasNonNullField(row, "attachments")) {
            List<Row> attachmentRows = row.getList(row.fieldIndex("attachments"));
            for (Row attachmentRow : attachmentRows) {
                String attachmentId = null;
                String parentAttachmentId = null;
                String fileName = null;
                if (hasNonNullField(attachmentRow, "metadata")) {
                    Row metaRow = attachmentRow.getAs("metadata");
                    attachmentId = getStringOrNull(metaRow, "attachment_id");
                    parentAttachmentId = getStringOrNull(metaRow, "parent_attachment_id");
                    fileName = getStringOrNull(metaRow, "file_name");
                }
                String cleanText = null;
                if (hasNonNullField(attachmentRow, "content")) {
                    Row attContentRow = attachmentRow.getAs("content");
                    cleanText = getStringOrNull(attContentRow, "clean_text");
                }
                attachments.add(new MessageAttachment(attachmentId, parentAttachmentId, fileName, cleanText));
            }
        }

        MessageProcessing processing = null;
        if (hasNonNullField(row, "processing")) {
            Row processingRow = row.getAs("processing");
            processing = new MessageProcessing(
                    getStringOrNull(processingRow, "run_date"),
                    getStringOrNull(processingRow, "run_hour"));
        }

        return new ScanMessage(messageId, source, content, attachments, processing, datasetId, restricted);
    }

    private static boolean hasNonNullField(Row row, String fieldName) {
        int idx;
        try {
            idx = row.fieldIndex(fieldName);
        } catch (IllegalArgumentException e) {
            return false; // field not present in this row's schema at all
        }
        return !row.isNullAt(idx);
    }

    private static String getStringOrNull(Row row, String fieldName) {
        if (!hasNonNullField(row, fieldName)) {
            return null;
        }
        return row.getAs(fieldName);
    }
}
