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
 * directly.
 *
 * <p>{@code datasetId}/{@code restricted} are NOT present in the AVRO itself
 * — they are supplied by the caller ({@code MessageAvroReader}, which knows
 * which dataset/subfolder a given file was read from), not read from
 * {@code row}.
 */
public final class MessageRowConverter implements Serializable {

    private MessageRowConverter() {}

    public static ScanMessage fromRow(Row row, String datasetId, boolean restricted) {
        String messageId = getStringOrNull(row, AvroConstants.FIELD_MESSAGE_ID);
        MessageSource source = readSource(row);
        MessageContent content = readContent(row);
        List<MessageAttachment> attachments = readAttachments(row);
        MessageProcessing processing = readProcessing(row);

        return new ScanMessage(messageId, source, content, attachments, processing, datasetId, restricted);
    }

    private static MessageSource readSource(Row row) {
        if (!hasNonNullField(row, AvroConstants.FIELD_SOURCE)) {
            return null;
        }
        Row sourceRow = row.getAs(AvroConstants.FIELD_SOURCE);
        return new MessageSource(
                getStringOrNull(sourceRow, AvroConstants.FIELD_CHANNEL_NAME),
                getStringOrNull(sourceRow, AvroConstants.FIELD_SOURCE_NAME),
                getStringOrNull(sourceRow, AvroConstants.FIELD_SRC_SYS_NAME),
                getStringOrNull(sourceRow, AvroConstants.FIELD_SRC_SYS_CONV_ID));
    }

    private static MessageContent readContent(Row row) {
        if (!hasNonNullField(row, AvroConstants.FIELD_MESSAGE)) {
            return null;
        }
        Row messageRow = row.getAs(AvroConstants.FIELD_MESSAGE);
        if (!hasNonNullField(messageRow, AvroConstants.FIELD_CONTENT)) {
            return null;
        }
        Row contentRow = messageRow.getAs(AvroConstants.FIELD_CONTENT);
        return new MessageContent(
                getStringOrNull(contentRow, AvroConstants.FIELD_HEADER),
                getStringOrNull(contentRow, AvroConstants.FIELD_RAW_TEXT),
                getStringOrNull(contentRow, AvroConstants.FIELD_SUBJECT),
                getStringOrNull(contentRow, AvroConstants.FIELD_CLEAN_TEXT));
    }

    private static List<MessageAttachment> readAttachments(Row row) {
        List<MessageAttachment> attachments = new ArrayList<>();
        if (!hasNonNullField(row, AvroConstants.FIELD_ATTACHMENTS)) {
            return attachments;
        }
        List<Row> attachmentRows = row.getList(row.fieldIndex(AvroConstants.FIELD_ATTACHMENTS));
        for (Row attachmentRow : attachmentRows) {
            attachments.add(readAttachment(attachmentRow));
        }
        return attachments;
    }

    private static MessageAttachment readAttachment(Row attachmentRow) {
        String attachmentId = null;
        String parentAttachmentId = null;
        String fileName = null;
        if (hasNonNullField(attachmentRow, AvroConstants.FIELD_METADATA)) {
            Row metadataRow = attachmentRow.getAs(AvroConstants.FIELD_METADATA);
            attachmentId = getStringOrNull(metadataRow, AvroConstants.FIELD_ATTACHMENT_ID);
            parentAttachmentId = getStringOrNull(metadataRow, AvroConstants.FIELD_PARENT_ATTACHMENT_ID);
            fileName = getStringOrNull(metadataRow, AvroConstants.FIELD_FILE_NAME);
        }
        String cleanText = null;
        if (hasNonNullField(attachmentRow, AvroConstants.FIELD_CONTENT)) {
            Row attachmentContentRow = attachmentRow.getAs(AvroConstants.FIELD_CONTENT);
            cleanText = getStringOrNull(attachmentContentRow, AvroConstants.FIELD_CLEAN_TEXT);
        }
        return new MessageAttachment(attachmentId, parentAttachmentId, fileName, cleanText);
    }

    private static MessageProcessing readProcessing(Row row) {
        if (!hasNonNullField(row, AvroConstants.FIELD_PROCESSING)) {
            return null;
        }
        Row processingRow = row.getAs(AvroConstants.FIELD_PROCESSING);
        return new MessageProcessing(
                getStringOrNull(processingRow, AvroConstants.FIELD_RUN_DATE),
                getStringOrNull(processingRow, AvroConstants.FIELD_RUN_HOUR));
    }

    private static boolean hasNonNullField(Row row, String fieldName) {
        int fieldIndex;
        try {
            fieldIndex = row.fieldIndex(fieldName);
        } catch (IllegalArgumentException e) {
            return false; // field not present in this row's schema at all
        }
        return !row.isNullAt(fieldIndex);
    }

    private static String getStringOrNull(Row row, String fieldName) {
        if (!hasNonNullField(row, fieldName)) {
            return null;
        }
        return row.getAs(fieldName);
    }
}
