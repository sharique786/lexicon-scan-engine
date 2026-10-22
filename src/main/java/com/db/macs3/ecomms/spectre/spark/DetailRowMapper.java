package com.db.macs3.ecomms.spectre.spark;

import com.db.macs3.ecomms.spectre.bq.OutputTableWriter;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Row;

import java.io.Serial;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Maps each successful {@link MessageProcessingResult} in a partition to its
 * {@code lexicon-hit-restricted}/{@code -unrestricted} detail {@link Row},
 * filtered to the source ({@code restricted}) this instance was built for —
 * skips errored results and results with no detail row (see
 * {@code OutputRowBuilder#buildDetailRow} for why a success can still carry
 * a null detail row). One instance each for restricted/unrestricted — see
 * {@link ScanEngineJobRunner writeOutputs()}, which runs this via
 * {@code Dataset.mapPartitions} directly on the cached results
 * {@code Dataset}, rather than via {@code JavaRDD.map}.
 */
public final class DetailRowMapper implements MapPartitionsFunction<MessageProcessingResult, Row> {

    @Serial
    private static final long serialVersionUID = 1L;

    private final boolean restricted;

    public DetailRowMapper(boolean restricted) {
        this.restricted = restricted;
    }

    @Override
    public Iterator<Row> call(Iterator<MessageProcessingResult> input) {
        List<Row> rows = new ArrayList<>();
        while (input.hasNext()) {
            MessageProcessingResult result = input.next();
            if (!result.isError() && result.isRestricted() == restricted && result.getDetailRow() != null) {
                rows.add(OutputTableWriter.toRow(result.getDetailRow()));
            }
        }
        return rows.iterator();
    }
}
