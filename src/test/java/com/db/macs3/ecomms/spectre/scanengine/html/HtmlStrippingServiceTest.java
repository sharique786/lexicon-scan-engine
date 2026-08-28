package com.db.macs3.ecomms.spectre.scanengine.html;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("HtmlStrippingService")
class HtmlStrippingServiceTest {

    @Nested
    @DisplayName("the requirement's worked example")
    class WorkedExample {

        private static final String ORIGINAL = "<p>Enjoy</p>\n<p>Happy Birthday</p>";
        // Instance (not static) field: a non-constant static field is not permitted inside a
        // non-static inner class (like a JUnit @Nested class) until Java 16 — this project
        // targets Java 11, so Pattern.compile(...)'s result (not a compile-time constant,
        // unlike the plain String literal above) is held per-instance instead.
        private final Pattern PATTERN = Pattern.compile("Enjoy(?:\\s+\\S+){0,2}\\s+Happy");

        @Test
        @DisplayName("strips to ' Enjoy Happy Birthday ' — tags/whitespace runs collapse to one space each")
        void stripsCorrectly() {
            HtmlStrippingService.StripResult result = HtmlStrippingService.strip(ORIGINAL);
            assertThat(result.strippedText()).isEqualTo(" Enjoy Happy Birthday ");
        }

        @Test
        @DisplayName("matchedText is 'Enjoy Happy', matching the requirement exactly")
        void matchedTextIsCorrect() {
            HtmlStrippingService.StripResult result = HtmlStrippingService.strip(ORIGINAL);
            Matcher m = PATTERN.matcher(result.strippedText());
            assertThat(m.find()).isTrue();
            assertThat(m.group()).isEqualTo("Enjoy Happy");
        }

        @Test
        @DisplayName("startCharIndex is 3, matching the requirement exactly")
        void startCharIndexIsCorrect() {
            HtmlStrippingService.StripResult result = HtmlStrippingService.strip(ORIGINAL);
            Matcher m = PATTERN.matcher(result.strippedText());
            m.find();
            int originalStart = result.offsetMap().toOriginal(m.start());
            assertThat(originalStart).isEqualTo(3);
        }

        @Test
        @DisplayName("endCharIndex is 21 (internally consistent — see class Javadoc's precise " +
                     "note on the requirement's stated 22, which multiple independent " +
                     "derivations do not reproduce)")
        void endCharIndexIsInternallyConsistent() {
            HtmlStrippingService.StripResult result = HtmlStrippingService.strip(ORIGINAL);
            Matcher m = PATTERN.matcher(result.strippedText());
            m.find();
            int originalEnd = result.offsetMap().toOriginal(m.end());
            assertThat(originalEnd).isEqualTo(21);
        }
    }

    @Nested
    @DisplayName("plain text with no HTML")
    class PlainText {

        private static final String PLAIN = "he's going to bomb the entire business unit";

        @Test
        @DisplayName("stripping is a no-op — identical to the original")
        void isNoOp() {
            assertThat(HtmlStrippingService.strip(PLAIN).strippedText()).isEqualTo(PLAIN);
        }

        @Test
        @DisplayName("matchedText equals original.substring(start,end) exactly, with no HTML to skip over")
        void matchedTextEqualsSubstring() {
            HtmlStrippingService.StripResult result = HtmlStrippingService.strip(PLAIN);
            Matcher m = Pattern.compile("bomb").matcher(result.strippedText());
            m.find();
            int start = result.offsetMap().toOriginal(m.start());
            int end = result.offsetMap().toOriginal(m.end());
            assertThat(PLAIN.substring(start, end)).isEqualTo("bomb");
        }
    }

    @Nested
    @DisplayName("edge cases")
    class EdgeCases {

        @Test
        @DisplayName("empty string produces empty stripped text")
        void handlesEmptyString() {
            assertThat(HtmlStrippingService.strip("").strippedText()).isEmpty();
        }

        @Test
        @DisplayName("null input produces empty stripped text, not an exception")
        void handlesNull() {
            assertThat(HtmlStrippingService.strip(null).strippedText()).isEmpty();
        }

        @Test
        @DisplayName("adjacent tags with no separating whitespace collapse to ONE space, not several")
        void collapsesAdjacentTags() {
            assertThat(HtmlStrippingService.strip("<div><span>hello</span></div>").strippedText())
                    .isEqualTo(" hello ");
        }

        @Test
        @DisplayName("a tag with attributes is stripped as one unit")
        void handlesTagAttributes() {
            assertThat(HtmlStrippingService.strip("<span class=\"x\">word</span>").strippedText())
                    .isEqualTo(" word ");
        }

        @Test
        @DisplayName("a stray '<' with no closing '>' is preserved as literal text, not treated as a tag")
        void preservesStrayAngleBracket() {
            String strayLt = "if a < b then bomb";
            assertThat(HtmlStrippingService.strip(strayLt).strippedText()).isEqualTo(strayLt);
        }
    }
}
