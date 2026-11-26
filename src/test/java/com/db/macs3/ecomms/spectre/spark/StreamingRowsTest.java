package com.db.macs3.ecomms.spectre.spark;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

class StreamingRowsTest {

    @Test
    void skipsNullsAndIsLazy() {
        List<Integer> pulled = new ArrayList<>();
        Iterator<MessageProcessingResult> input = List.of(
                MessageProcessingResult.failure("a", false, "2026-01-01", "x"),
                MessageProcessingResult.failure("b", false, "2026-01-01", "x"),
                MessageProcessingResult.failure("c", false, "2026-01-01", "x")).iterator();

        Iterator<Row> out = StreamingRows.map(input, r -> {
            pulled.add(1);
            return r.getMessageId().equals("b") ? null : RowFactory.create(r.getMessageId());
        });

        assertEquals(0, pulled.size()); // nothing consumed until asked
        assertEquals("a", out.next().getString(0));
        assertEquals(1, pulled.size());
        assertEquals("c", out.next().getString(0)); // "b" skipped
        assertFalse(out.hasNext());
        assertThrows(NoSuchElementException.class, out::next);
    }
}
