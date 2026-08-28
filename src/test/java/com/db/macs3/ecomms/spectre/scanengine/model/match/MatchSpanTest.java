package com.db.macs3.ecomms.spectre.scanengine.model.match;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("MatchSpan")
class MatchSpanTest {

    @Test
    @DisplayName("length() is endCharIndex - startCharIndex")
    void computesLength() {
        assertThat(new MatchSpan(10, 14, "bomb").length()).isEqualTo(4);
    }

    @Test
    @DisplayName("rejects a negative startCharIndex")
    void rejectsNegativeStart() {
        assertThatThrownBy(() -> new MatchSpan(-1, 4, "x")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("rejects endCharIndex before startCharIndex")
    void rejectsEndBeforeStart() {
        assertThatThrownBy(() -> new MatchSpan(10, 5, "x")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("isFullyContainedIn: a span entirely inside another returns true")
    void fullyContainedReturnsTrue() {
        MatchSpan disclaimer = new MatchSpan(10, 34, "confidential information");
        MatchSpan lexiconMatch = new MatchSpan(15, 27, "information");
        assertThat(lexiconMatch.isFullyContainedIn(disclaimer)).isTrue();
    }

    @Test
    @DisplayName("isFullyContainedIn: a PARTIALLY overlapping span returns false (full containment only, per requirement 8.b)")
    void partialOverlapReturnsFalse() {
        MatchSpan disclaimer = new MatchSpan(10, 34, "confidential information");
        MatchSpan partiallyOverlapping = new MatchSpan(30, 40, "partial");
        assertThat(partiallyOverlapping.isFullyContainedIn(disclaimer)).isFalse();
    }

    @Test
    @DisplayName("isFullyContainedIn: a span entirely outside returns false")
    void entirelyOutsideReturnsFalse() {
        MatchSpan disclaimer = new MatchSpan(10, 34, "confidential information");
        MatchSpan outside = new MatchSpan(50, 54, "bomb");
        assertThat(outside.isFullyContainedIn(disclaimer)).isFalse();
    }

    @Test
    @DisplayName("isFullyContainedIn: identical spans are mutually contained")
    void identicalSpansAreContained() {
        MatchSpan a = new MatchSpan(10, 20, "x");
        MatchSpan b = new MatchSpan(10, 20, "x");
        assertThat(a.isFullyContainedIn(b)).isTrue();
    }
}
