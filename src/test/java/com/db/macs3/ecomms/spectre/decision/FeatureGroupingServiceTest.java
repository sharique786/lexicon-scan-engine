package com.db.macs3.ecomms.spectre.decision;

import com.db.macs3.ecomms.spectre.model.decision.FeatureGroup;
import com.db.macs3.ecomms.spectre.model.view.FeatureDecisionRow;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("FeatureGroupingService")
class FeatureGroupingServiceTest {

    private static final LocalDate SOME_DATE = LocalDate.parse("2026-08-16");

    private static FeatureDecisionRow row(String featureId, String featureType, String featureName,
                                           String subFeatureType, String featuresToApply,
                                           boolean isNoiseReduction, String operator) {
        return new FeatureDecisionRow("proc-1", "msg-101", SOME_DATE, "Lexicon-Tagging",
                featureType, Long.parseLong(featureId), featureName, subFeatureType, featuresToApply,
                isNoiseReduction, operator, "{}", SOME_DATE, "101");
    }

    @Nested
    @DisplayName("grouping and processing order (matching the Excel View Data sample structure)")
    class GroupingAndOrder {

        // msg-101 scenario: featureId=1 standalone lexicon, featureId=2 disclaimer,
        // featureId=3 composite/NoiseReduction with 2 OR'd sub-features.
        private final List<FeatureDecisionRow> rows = List.of(
                row("1", "lexicon", "lexicon_mkt_cond_1", null, "lexicon_mkt_cond_1", false, null),
                row("2", "disclaimer", "std_disclaimer_1", null, "std_disclaimer_1", false, null),
                row("3", "composite", "NotNewsLetter", "lexicon", "lexicon_mkt_cond_2", true, "OR"),
                row("3", "composite", "NotNewsLetter", "lexicon", "lexicon_mkt_cond_3", true, "OR")
        );
        private final List<FeatureGroup> groups = FeatureGroupingService.groupAndOrder(rows);

        @Test
        @DisplayName("4 rows group into 3 distinct featureId groups")
        void groupsCorrectly() {
            assertThat(groups).hasSize(3);
        }

        @Test
        @DisplayName("processing order is NoiseReduction first")
        void noiseReductionFirst() {
            assertThat(groups.get(0).getFeatureId()).isEqualTo("3");
            assertThat(groups.get(0).isNoiseReduction()).isTrue();
        }

        @Test
        @DisplayName("processing order is Disclaimer second")
        void disclaimerSecond() {
            assertThat(groups.get(1).getFeatureId()).isEqualTo("2");
            assertThat(groups.get(1).isDisclaimer()).isTrue();
        }

        @Test
        @DisplayName("processing order is standard Lexicon last")
        void lexiconLast() {
            assertThat(groups.get(2).getFeatureId()).isEqualTo("1");
        }

        @Test
        @DisplayName("the NoiseReduction group has both members and the OR operator")
        void noiseReductionGroupDetails() {
            FeatureGroup nrGroup = groups.get(0);
            assertThat(nrGroup.getMembers()).hasSize(2);
            assertThat(nrGroup.getOperator()).isEqualTo("OR");
            assertThat(nrGroup.isMultiMember()).isTrue();
        }

        @Test
        @DisplayName("a single-member group has a null operator (meaningless for one member)")
        void singleMemberGroupHasNullOperator() {
            assertThat(groups.get(1).getOperator()).isNull();
            assertThat(groups.get(1).isMultiMember()).isFalse();
        }
    }

    @Test
    @DisplayName("a NoiseReduction group with AND operator is parsed correctly")
    void parsesAndOperatorGroup() {
        List<FeatureGroup> groups = FeatureGroupingService.groupAndOrder(List.of(
                row("5", "NoiseReduction", "NotNewsLetter2", "lexicon", "lexicon_spam_1", true, "AND"),
                row("5", "NoiseReduction", "NotNewsLetter2", "lexicon", "lexicon_spam_2", true, "AND")
        ));
        assertThat(groups).hasSize(1);
        assertThat(groups.get(0).getOperator()).isEqualTo("AND");
    }

    @Nested
    @DisplayName("validation")
    class Validation {

        @Test
        @DisplayName("inconsistent featureType across rows sharing a featureId throws")
        void rejectsInconsistentFeatureType() {
            List<FeatureDecisionRow> badRows = List.of(
                    row("9", "lexicon", "X", null, "x1", false, null),
                    row("9", "composite", "X", null, "x2", false, null)
            );
            assertThatThrownBy(() -> FeatureGroupingService.groupAndOrder(badRows))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        @DisplayName("a multi-member group with no operator throws")
        void rejectsMissingOperator() {
            List<FeatureDecisionRow> badRows = List.of(
                    row("10", "composite", "Y", "lexicon", "a", true, null),
                    row("10", "composite", "Y", "lexicon", "b", true, null)
            );
            assertThatThrownBy(() -> FeatureGroupingService.groupAndOrder(badRows))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Nested
    @DisplayName("empty/null input")
    class EmptyInput {

        @Test
        @DisplayName("empty list returns empty groups")
        void handlesEmptyList() {
            assertThat(FeatureGroupingService.groupAndOrder(List.of())).isEmpty();
        }

        @Test
        @DisplayName("null input returns empty groups")
        void handlesNull() {
            assertThat(FeatureGroupingService.groupAndOrder(null)).isEmpty();
        }
    }
}
