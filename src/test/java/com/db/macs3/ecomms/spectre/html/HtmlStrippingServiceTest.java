package com.db.macs3.ecomms.spectre.html;

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
            Matcher matcher = PATTERN.matcher(result.strippedText());
            assertThat(matcher.find()).isTrue();
            assertThat(matcher.group()).isEqualTo("Enjoy Happy");
        }

        @Test
        @DisplayName("startCharIndex is 3, matching the requirement exactly")
        void startCharIndexIsCorrect() {
            HtmlStrippingService.StripResult result = HtmlStrippingService.strip(ORIGINAL);
            Matcher matcher = PATTERN.matcher(result.strippedText());
            matcher.find();
            int originalStart = result.offsetMap().toOriginal(matcher.start());
            assertThat(originalStart).isEqualTo(3);
        }

        @Test
        @DisplayName("endCharIndex is 21 (internally consistent — see class Javadoc's precise " +
                     "note on the requirement's stated 22, which multiple independent " +
                     "derivations do not reproduce)")
        void endCharIndexIsInternallyConsistent() {
            HtmlStrippingService.StripResult result = HtmlStrippingService.strip(ORIGINAL);
            Matcher matcher = PATTERN.matcher(result.strippedText());
            matcher.find();
            int originalEnd = result.offsetMap().toOriginal(matcher.end());
            assertThat(originalEnd).isEqualTo(21);
        }
    }

    @Nested
    @DisplayName("clean-text normalisation: newlines, tabs, commas and repeated whitespace -> one space")
    class CleanTextNormalisation {

        @Test
        @DisplayName("newlines, tabs, CRLF and runs of spaces each collapse to exactly one space")
        void whitespaceKindsCollapse() {
            assertThat(HtmlStrippingService.strip("a\nb\tc\r\nd   e \n\t f").strippedText())
                    .isEqualTo("a b c d e f");
        }

        @Test
        @DisplayName("a comma becomes a space, and merges with adjacent whitespace/commas/tags into ONE space")
        void commasCollapse() {
            assertThat(HtmlStrippingService.strip("keep,in,the market").strippedText()).isEqualTo("keep in the market");
            assertThat(HtmlStrippingService.strip("one,\n  two ,, three").strippedText()).isEqualTo("one two three");
            assertThat(HtmlStrippingService.strip("<p>Hello</p>, <p>world</p>").strippedText())
                    .isEqualTo(" Hello world ");
        }

        @Test
        @DisplayName("fullwidth (CJK) and Arabic commas are treated as commas too")
        void nonAsciiCommas() {
            assertThat(HtmlStrippingService.strip("股票，市场").strippedText())
                    .isEqualTo("股票 市场");
            assertThat(HtmlStrippingService.strip("واحد، اثنان").strippedText())
                    .isEqualTo("واحد اثنان");
        }

        @Test
        @DisplayName("a term needing a space between words now matches across a comma or newline")
        void proximityPatternMatchesAcrossCommaAndNewline() {
            Pattern pattern = Pattern.compile("keep(?:\\s+\\S+){0,3}\\s+mouth shut");
            assertThat(pattern.matcher(HtmlStrippingService.strip("keep,\nin the market, mouth\nshut").strippedText())
                    .find()).isTrue();
        }

        @Test
        @DisplayName("offset map still resolves match boundaries to the ORIGINAL text after comma collapsing")
        void offsetMapStaysCorrect() {
            String original = "alpha,,  beta\n\ngamma";
            HtmlStrippingService.StripResult result = HtmlStrippingService.strip(original);
            assertThat(result.strippedText()).isEqualTo("alpha beta gamma");
            int betaStart = result.strippedText().indexOf("beta");
            int betaEnd = betaStart + "beta".length();
            assertThat(original.substring(result.offsetMap().toOriginal(betaStart),
                    result.offsetMap().toOriginal(betaEnd))).isEqualTo("beta");
            int gammaStart = result.strippedText().indexOf("gamma");
            assertThat(original.substring(result.offsetMap().toOriginal(gammaStart))).isEqualTo("gamma");
        }

        @Test
        @DisplayName("CHAT/VOICE path: message_text cells are normalised the same way, offsets map to raw_text")
        void chatVoiceExtractionPathNormalisesToo() {
            String raw = "<table><tr><td class=\"message-text-cell message_text\">keep,\n\tin the, market</td></tr></table>";
            ChatVoiceMessageTextExtractor.ExtractionResult extraction = ChatVoiceMessageTextExtractor.extract(raw);
            assertThat(extraction.anyCellFound()).isTrue();

            HtmlStrippingService.StripResult result =
                    HtmlStrippingService.stripExtracted(extraction.extractedText(), extraction.offsetMap());
            assertThat(result.strippedText()).isEqualTo("keep in the market");
            int marketStart = result.strippedText().indexOf("market");
            assertThat(raw.substring(result.offsetMap().toOriginal(marketStart),
                    result.offsetMap().toOriginal(marketStart + "market".length()))).isEqualTo("market");
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
            Matcher matcher = Pattern.compile("bomb").matcher(result.strippedText());
            matcher.find();
            int start = result.offsetMap().toOriginal(matcher.start());
            int end = result.offsetMap().toOriginal(matcher.end());
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

    @Nested
    @DisplayName("identity() — the zero-allocation passthrough for content known to need no stripping")
    class Identity {

        @Test
        @DisplayName("strippedText is the input verbatim, even for text strip() WOULD change — " +
                     "proving identity() never runs the tag/whitespace-scanning algorithm at all")
        void strippedTextIsVerbatim() {
            String htmlBearing = "<p>Enjoy</p>\n<p>Happy Birthday</p>";
            HtmlStrippingService.StripResult result = HtmlStrippingService.identity(htmlBearing);
            assertThat(result.strippedText()).isEqualTo(htmlBearing);
        }

        @Test
        @DisplayName("toOriginal(x) == x for every position, matching a genuinely unchanged text")
        void offsetMapIsTrueIdentity() {
            String text = "manipulate the closing price";
            HtmlStrippingService.StripResult result = HtmlStrippingService.identity(text);
            for (int i = 0; i <= text.length(); i++) {
                assertThat(result.offsetMap().toOriginal(i)).isEqualTo(i);
            }
        }

        @Test
        @DisplayName("works correctly for a position far beyond any small fixed-size array — " +
                     "no int[] is ever allocated, so there is no length to be out of bounds of")
        void handlesArbitrarilyLargePositionsWithNoAllocation() {
            HtmlStrippingService.OffsetMap identity = HtmlStrippingService.OffsetMap.identity();
            assertThat(identity.toOriginal(10_000_000)).isEqualTo(10_000_000);
        }

        @Test
        @DisplayName("a negative position still throws, matching strip()'s own bounds contract")
        void negativePositionThrows() {
            HtmlStrippingService.OffsetMap identity = HtmlStrippingService.OffsetMap.identity();
            org.assertj.core.api.Assertions.assertThatThrownBy(() -> identity.toOriginal(-1))
                    .isInstanceOf(IndexOutOfBoundsException.class);
        }

        @Test
        @DisplayName("null input produces an empty result, matching strip()'s own null handling")
        void handlesNull() {
            assertThat(HtmlStrippingService.identity(null).strippedText()).isEmpty();
        }

        @Test
        @DisplayName("identity() and strip() agree on already-clean, HTML-free text")
        void agreesWithStripOnCleanText() {
            String clean = "insider trading occurred yesterday";
            assertThat(HtmlStrippingService.identity(clean).strippedText())
                    .isEqualTo(HtmlStrippingService.strip(clean).strippedText());
        }
    }

    @Nested
    @DisplayName("OffsetMap.of(int[]) — a caller-supplied boundaries array")
    class OffsetMapOf {

        @Test
        @DisplayName("toOriginal delegates straight to the supplied array")
        void delegatesToSuppliedArray() {
            HtmlStrippingService.OffsetMap map = HtmlStrippingService.OffsetMap.of(new int[]{5, 9, 20});
            assertThat(map.toOriginal(0)).isEqualTo(5);
            assertThat(map.toOriginal(1)).isEqualTo(9);
            assertThat(map.toOriginal(2)).isEqualTo(20);
        }

        @Test
        @DisplayName("null array throws")
        void nullArrayThrows() {
            org.assertj.core.api.Assertions.assertThatThrownBy(() -> HtmlStrippingService.OffsetMap.of(null))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        @DisplayName("empty array throws")
        void emptyArrayThrows() {
            org.assertj.core.api.Assertions.assertThatThrownBy(() -> HtmlStrippingService.OffsetMap.of(new int[0]))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Nested
    @DisplayName("stripExtracted — composing an extraction's own offset map with a further strip pass")
    class StripExtracted {

        @Test
        @DisplayName("a two-stage composition resolves straight back to the ORIGINAL document, "
                     + "never to the intermediate extracted text")
        void composesBothOffsetMaps() {
            // Simulates ChatVoiceMessageTextExtractor's shape: "extractedText" is itself a
            // substring pulled out of some larger original document, starting at offset 100
            // there, and still carries its own inner HTML tag to be stripped.
            String extractedText = "Can you share <p>market</p> price?";
            int[] extractionBoundaries = new int[extractedText.length() + 1];
            for (int i = 0; i <= extractedText.length(); i++) {
                extractionBoundaries[i] = 100 + i; // extractedText sits at original offset 100
            }
            HtmlStrippingService.OffsetMap extractionOffsetMap = HtmlStrippingService.OffsetMap.of(extractionBoundaries);

            HtmlStrippingService.StripResult result =
                    HtmlStrippingService.stripExtracted(extractedText, extractionOffsetMap);

            assertThat(result.strippedText()).isEqualTo("Can you share market price?");

            Matcher matcher = Pattern.compile("market price").matcher(result.strippedText());
            assertThat(matcher.find()).isTrue();
            int originalStart = result.offsetMap().toOriginal(matcher.start());
            int originalEnd = result.offsetMap().toOriginal(matcher.end());
            // Position in the ORIGINAL document (offset 100 + extractedText's own local offset),
            // not in extractedText itself.
            assertThat(originalStart).isEqualTo(100 + extractedText.indexOf("market"));
        }
    }
}
