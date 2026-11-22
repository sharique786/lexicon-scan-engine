package com.gliwka.hyperscan.wrapper;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link CollapsingScanner.MatchCollector} replaced a {@code LinkedHashMap<Long, long[]>}: it must keep exactly the
 * same semantics — longest end per (id, start), first-seen order — through growth and reuse.
 */
@DisplayName("CollapsingScanner.MatchCollector")
class MatchCollectorTest {

    private static List<long[]> entries(CollapsingScanner.MatchCollector c) {
        List<long[]> out = new ArrayList<>();
        for (int i = 0; i < c.size(); i++) {
            out.add(new long[]{c.id(i), c.from(i), c.to(i)});
        }
        return out;
    }

    @Test
    @DisplayName("keeps the longest end per (id, start), in first-seen order")
    void keepsLongestEndInFirstSeenOrder() {
        CollapsingScanner.MatchCollector c = new CollapsingScanner.MatchCollector();
        c.add(2, 10, 15);
        c.add(1, 0, 3);
        c.add(2, 10, 30);   // longer end, same key
        c.add(2, 10, 20);   // shorter end: ignored
        c.add(1, 5, 9);
        c.add(2, 0, 4);     // same start as id 1's first match, different id -> separate entry

        assertThat(entries(c)).containsExactly(
                new long[]{2, 10, 30}, new long[]{1, 0, 3}, new long[]{1, 5, 9}, new long[]{2, 0, 4});
    }

    @Test
    @DisplayName("matches a LinkedHashMap reference through many rehashes")
    void agreesWithLinkedHashMapReference() {
        CollapsingScanner.MatchCollector c = new CollapsingScanner.MatchCollector();
        Map<Long, long[]> reference = new LinkedHashMap<>();
        Random random = new Random(42);
        for (int i = 0; i < 200_000; i++) {
            int id = random.nextInt(50);
            long from = random.nextInt(20_000);
            long to = from + random.nextInt(100);
            c.add(id, from, to);
            long key = ((long) id << 32) | (from & 0xFFFFFFFFL);
            long[] e = reference.get(key);
            if (e == null) {
                reference.put(key, new long[]{id, from, to});
            } else if (to > e[2]) {
                e[2] = to;
            }
        }
        assertThat(entries(c)).containsExactlyElementsOf(new ArrayList<>(reference.values()));
    }

    @Test
    @DisplayName("clear() empties it for reuse, including after it grew large")
    void clearAllowsReuse() {
        CollapsingScanner.MatchCollector c = new CollapsingScanner.MatchCollector();
        for (int i = 0; i < 100_000; i++) {
            c.add(1, i, i + 1);
        }
        c.clear();
        assertThat(c.isEmpty()).isTrue();
        c.add(7, 3, 4);
        c.add(7, 3, 8);
        assertThat(entries(c)).containsExactly(new long[]{7, 3, 8});

        c.clear();
        c.add(1, 1, 2);
        assertThat(entries(c)).containsExactly(new long[]{1, 1, 2});
    }

    @Test
    @DisplayName("start offsets beyond 32 bits keep the old key semantics (low 32 bits of the start)")
    void keyUsesLow32BitsOfStart() {
        CollapsingScanner.MatchCollector c = new CollapsingScanner.MatchCollector();
        c.add(1, 5L, 6L);
        c.add(1, (1L << 32) + 5L, 9L); // same key as before under the old LinkedHashMap scheme
        assertThat(c.size()).isEqualTo(1);
        assertThat(c.to(0)).isEqualTo(9L);
    }
}
