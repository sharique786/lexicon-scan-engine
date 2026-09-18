package com.db.macs3.ecomms.spectre.bq;

import com.db.macs3.ecomms.spectre.constants.BqColumns;
import com.db.macs3.ecomms.spectre.model.output.LexiconHitDetailRow;
import com.db.macs3.ecomms.spectre.model.output.LexiconHitSummaryRow;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDate;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies {@code evaluated_lexicons.id} is written as an INTEGER (Spark
 * {@code LongType}) in all three tables sharing that column — this was
 * changed from STRING in a later revision; see
 * {@link LexiconHitSummaryRow.EvaluatedLexicon}/{@link LexiconHitDetailRow.EvaluatedLexicon}
 * class Javadoc.
 */
@DisplayName("OutputTableWriter")
class OutputTableWriterTest {

    private static final Instant NOW = Instant.parse("2026-08-17T10:00:00Z");
    private static final LocalDate SOME_DATE = LocalDate.parse("2026-08-16");

    private static DataType fieldType(StructType struct, String fieldName) {
        for (StructField field : struct.fields()) {
            if (field.name().equals(fieldName)) {
                return field.dataType();
            }
        }
        throw new AssertionError("No field named '" + fieldName + "' in " + struct);
    }

    private static StructType elementStruct(DataType arrayField) {
        return (StructType) ((ArrayType) arrayField).elementType();
    }

    @Nested
    @DisplayName("evaluated_lexicons.id is INTEGER (LongType), not STRING, in every table's schema")
    class EvaluatedLexiconsIdType {

        @Test
        @DisplayName("lexicon-hit-summary")
        void lexiconHitSummary() {
            DataType evaluatedLexiconsField = fieldType(
                    OutputTableWriter.LEXICON_HIT_SUMMARY_SCHEMA, BqColumns.LexiconHitSummary.EVALUATED_LEXICONS);
            StructType evaluatedLexiconStruct = elementStruct(evaluatedLexiconsField);
            assertThat(fieldType(evaluatedLexiconStruct, BqColumns.LexiconHitSummary.EvaluatedLexicon.ID))
                    .isEqualTo(DataTypes.LongType);
        }

        @Test
        @DisplayName("lexicon-hit-restricted / lexicon-hit-unrestricted (shared schema)")
        void lexiconHitDetail() {
            DataType evaluatedLexiconsField = fieldType(
                    OutputTableWriter.LEXICON_HIT_DETAIL_SCHEMA, BqColumns.LexiconHitDetail.EVALUATED_LEXICONS);
            StructType evaluatedLexiconStruct = elementStruct(evaluatedLexiconsField);
            assertThat(fieldType(evaluatedLexiconStruct, BqColumns.LexiconHitDetail.EvaluatedLexicon.ID))
                    .isEqualTo(DataTypes.LongType);
        }
    }

    @Nested
    @DisplayName("toRow(...) carries a genuine Long into the id cell, not a String")
    class ToRowIdValue {

        @Test
        @DisplayName("LexiconHitSummaryRow")
        void summaryRowIdIsLong() {
            LexiconHitSummaryRow.EvaluatedLexicon lexicon = new LexiconHitSummaryRow.EvaluatedLexicon(
                    7L, "lex_name", 5L, 1L,
                    List.of(new LexiconHitSummaryRow.TermDtl("lex_name::1", "pattern", 1L)));
            LexiconHitSummaryRow summaryRow = new LexiconHitSummaryRow(
                    "msg-1", "proc-1", "pipe-1", List.of(lexicon), SOME_DATE, "scan-engine", NOW);

            Row row = OutputTableWriter.toRow(summaryRow);
            Row lexiconRow = row.<scala.collection.Seq<Row>>getAs(3).apply(0);
            assertThat(lexiconRow.get(0)).isInstanceOf(Long.class).isEqualTo(7L);
        }

        @Test
        @DisplayName("LexiconHitDetailRow")
        void detailRowIdIsLong() {
            LexiconHitDetailRow.EvaluatedLexicon lexicon = new LexiconHitDetailRow.EvaluatedLexicon(
                    9L, List.of(new LexiconHitDetailRow.EvaluatedLexicon.TermDtl("lex_name::1", "{}")));
            LexiconHitDetailRow detailRow = new LexiconHitDetailRow(
                    "msg-1", "proc-1", "pipe-1", List.of(lexicon), SOME_DATE, "scan-engine", NOW);

            Row row = OutputTableWriter.toRow(detailRow);
            Row lexiconRow = row.<scala.collection.Seq<Row>>getAs(3).apply(0);
            assertThat(lexiconRow.get(0)).isInstanceOf(Long.class).isEqualTo(9L);
        }
    }
}
