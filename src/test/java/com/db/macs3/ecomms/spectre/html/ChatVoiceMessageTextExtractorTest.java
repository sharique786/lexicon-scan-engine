package com.db.macs3.ecomms.spectre.html;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("ChatVoiceMessageTextExtractor")
class ChatVoiceMessageTextExtractorTest {

    @Nested
    @DisplayName("the requirement's worked example")
    class WorkedExample {

        /**
         * The exact CHAT-type {@code content.raw_text} sample from the requirement — 3 rows,
         * only the last two of which have a non-empty {@code message_text} cell.
         */
        private static final String RAW_TEXT =
                "<html><head><style>.message-text-cell {white-space: pre-wrap;word-break: break-word;}"
                + "</style></head><body><table border=\"1\" cellpadding=\"5\" cellspacing=\"0\"><thead><tr>"
                + "<th>room_name</th><th>event_trigg_from</th><th>company_name</th><th>evt_id</th>"
                + "<th>msg_type</th><th>event_time</th><th>email</th><th>message_text</th>"
                + "<th>event_type</th><th>srcSysAttId</th><th>att_filename</th><th>att_file_size</th>"
                + "<th>broadcast_id</th></tr></thead><tbody><tr><td class=\"room_name\"></td>"
                + "<td class=\"event_trigg_from\"></td><td class=\"company_name\">DEUTSCHE BANK AG, LO</td>"
                + "<td class=\"evt_id\">34644ea1-b6af-3448-8a5e-74e7eab27434</td><td class=\"msg_type\"></td>"
                + "<td class=\"event_time\">2016-07-13T07:14:09Z</td>"
                + "<td class=\"email\">lars.van-leeuwenstijn@db.com</td>"
                + "<td class=\"message-text-cell message_text\"></td><td class=\"event_type\">Attachment</td>"
                + "<td class=\"srcSysAttId\">File_0.xlsx</td><td class=\"att_filename\">File_0.xlsx</td>"
                + "<td class=\"att_file_size\">30</td><td class=\"broadcast_id\"></td></tr>  <tr>"
                + "<td class=\"room_name\"></td><td class=\"event_trigg_from\"></td>"
                + "<td class=\"company_name\">DEUTSCHE BANK SECURI</td>"
                + "<td class=\"evt_id\">2f48e35e-5997-3191-99e9-8c1789b3db90</td><td class=\"msg_type\"></td>"
                + "<td class=\"event_time\">2016-09-28T13:08:14Z</td>"
                + "<td class=\"email\">aakanksha.kadam@db.com</td>"
                + "<td class=\"message-text-cell message_text\">Hi There</td>"
                + "<td class=\"event_type\">Message</td><td class=\"srcSysAttId\"></td>"
                + "<td class=\"att_filename\"></td><td class=\"att_file_size\"></td>"
                + "<td class=\"broadcast_id\"></td></tr><tr><td class=\"room_name\"></td>"
                + "<td class=\"event_trigg_from\"></td><td class=\"company_name\">DEUTSCHE BANK SECURI</td>"
                + "<td class=\"evt_id\">2f48e35e-5997-3191-99e9-8c1789b3db90</td><td class=\"msg_type\"></td>"
                + "<td class=\"event_time\">2016-09-28T13:08:14Z</td>"
                + "<td class=\"email\">aakanksha.kadam@db.com</td>"
                + "<td class=\"message-text-cell message_text\"><p>Can you share market price?</p></td>"
                + "<td class=\"event_type\">Message</td><td class=\"srcSysAttId\"></td>"
                + "<td class=\"att_filename\"></td><td class=\"att_file_size\"></td>"
                + "<td class=\"broadcast_id\"></td></tr></tbody></table></body></html>";

        @Test
        @DisplayName("finds a cell for every message_text <td>, including empty ones")
        void anyCellFoundIsTrue() {
            assertThat(ChatVoiceMessageTextExtractor.extract(RAW_TEXT).anyCellFound()).isTrue();
        }

        @Test
        @DisplayName("extractedText, once further stripped, is 'Hi There Can you share market price?' "
                     + "— company_name/email/evt_id/etc. never appear")
        void extractedAndStrippedTextMatchesRequirement() {
            ChatVoiceMessageTextExtractor.ExtractionResult extraction =
                    ChatVoiceMessageTextExtractor.extract(RAW_TEXT);
            String cleanText = HtmlStrippingService.stripExtracted(
                    extraction.extractedText(), extraction.offsetMap()).strippedText();

            assertThat(cleanText).contains("Hi There Can you share market price?");
            assertThat(cleanText).doesNotContain("DEUTSCHE BANK", "aakanksha", "34644ea1", "File_0.xlsx");
        }

        @Test
        @DisplayName("'market price' resolves back to raw_text at an EXACT substring match — "
                     + "no HTML markup in between, since it's a single unbroken run in one cell")
        void matchedTextPositionResolvesExactlyToRawText() {
            ChatVoiceMessageTextExtractor.ExtractionResult extraction =
                    ChatVoiceMessageTextExtractor.extract(RAW_TEXT);
            HtmlStrippingService.StripResult stripResult =
                    HtmlStrippingService.stripExtracted(extraction.extractedText(), extraction.offsetMap());

            String cleanText = stripResult.strippedText();
            int localStart = cleanText.indexOf("market price");
            assertThat(localStart).isGreaterThanOrEqualTo(0);
            int localEnd = localStart + "market price".length();

            int rawStart = stripResult.offsetMap().toOriginal(localStart);
            int rawEnd = stripResult.offsetMap().toOriginal(localEnd);
            assertThat(RAW_TEXT.substring(rawStart, rawEnd)).isEqualTo("market price");
        }
    }

    @Nested
    @DisplayName("class attribute matching")
    class ClassAttributeMatching {

        @Test
        @DisplayName("matches regardless of token order in the class attribute")
        void matchesRegardlessOfTokenOrder() {
            String rawText = "<table><tr><td class=\"message_text message-text-cell\">hello</td></tr></table>";
            ChatVoiceMessageTextExtractor.ExtractionResult extraction = ChatVoiceMessageTextExtractor.extract(rawText);
            assertThat(extraction.anyCellFound()).isTrue();
            assertThat(extraction.extractedText()).isEqualTo("hello");
        }

        @Test
        @DisplayName("does NOT match a class that merely CONTAINS the token as a substring, e.g. "
                     + "'message-text-cell' alone without the exact 'message_text' token")
        void doesNotMatchSubstringOnlyClass() {
            String rawText = "<table><tr><td class=\"message-text-cell\">hello</td></tr></table>";
            assertThat(ChatVoiceMessageTextExtractor.extract(rawText).anyCellFound()).isFalse();
        }

        @Test
        @DisplayName("single-quoted class attribute is supported too")
        void supportsSingleQuotedClassAttribute() {
            String rawText = "<table><tr><td class='message-text-cell message_text'>hello</td></tr></table>";
            ChatVoiceMessageTextExtractor.ExtractionResult extraction = ChatVoiceMessageTextExtractor.extract(rawText);
            assertThat(extraction.anyCellFound()).isTrue();
            assertThat(extraction.extractedText()).isEqualTo("hello");
        }
    }

    @Nested
    @DisplayName("multiple cells")
    class MultipleCells {

        @Test
        @DisplayName("concatenates every matching cell's content, in document order, separated by a space")
        void concatenatesInDocumentOrder() {
            String rawText = "<table>"
                    + "<tr><td class=\"message_text\">This is very strong language</td></tr>"
                    + "<tr><td class=\"message_text\">Cats and dogs hate each other</td></tr>"
                    + "</table>";
            ChatVoiceMessageTextExtractor.ExtractionResult extraction = ChatVoiceMessageTextExtractor.extract(rawText);
            assertThat(extraction.extractedText()).isEqualTo("This is very strong language Cats and dogs hate each other");
        }

        @Test
        @DisplayName("each cell's own content maps back to ITS OWN position in raw_text, not the other cell's")
        void eachCellMapsToItsOwnPosition() {
            String rawText = "<table>"
                    + "<tr><td class=\"message_text\">alpha</td></tr>"
                    + "<tr><td class=\"message_text\">beta</td></tr>"
                    + "</table>";
            ChatVoiceMessageTextExtractor.ExtractionResult extraction = ChatVoiceMessageTextExtractor.extract(rawText);
            HtmlStrippingService.StripResult stripResult =
                    HtmlStrippingService.stripExtracted(extraction.extractedText(), extraction.offsetMap());
            String cleanText = stripResult.strippedText();

            int alphaStart = cleanText.indexOf("alpha");
            int betaStart = cleanText.indexOf("beta");
            int rawAlphaStart = stripResult.offsetMap().toOriginal(alphaStart);
            int rawBetaStart = stripResult.offsetMap().toOriginal(betaStart);

            assertThat(rawText.substring(rawAlphaStart, rawAlphaStart + 5)).isEqualTo("alpha");
            assertThat(rawText.substring(rawBetaStart, rawBetaStart + 4)).isEqualTo("beta");
        }

        @Test
        @DisplayName("an empty cell contributes nothing but still keeps document order intact")
        void emptyCellContributesNothing() {
            String rawText = "<table>"
                    + "<tr><td class=\"message_text\"></td></tr>"
                    + "<tr><td class=\"message_text\">Hi There</td></tr>"
                    + "</table>";
            ChatVoiceMessageTextExtractor.ExtractionResult extraction = ChatVoiceMessageTextExtractor.extract(rawText);
            String cleanText = HtmlStrippingService.stripExtracted(
                    extraction.extractedText(), extraction.offsetMap()).strippedText();
            assertThat(cleanText).contains("Hi There");
        }
    }

    @Nested
    @DisplayName("no message_text cell at all")
    class NoMessageTextCell {

        @Test
        @DisplayName("anyCellFound is false for plain, non-table text")
        void falseForPlainText() {
            assertThat(ChatVoiceMessageTextExtractor.extract("just a plain message, no HTML at all").anyCellFound())
                    .isFalse();
        }

        @Test
        @DisplayName("anyCellFound is false for a table with no message_text column")
        void falseForTableWithoutMessageTextColumn() {
            String rawText = "<table><tr><td class=\"company_name\">DEUTSCHE BANK</td></tr></table>";
            assertThat(ChatVoiceMessageTextExtractor.extract(rawText).anyCellFound()).isFalse();
        }
    }

    @Nested
    @DisplayName("edge cases")
    class EdgeCases {

        @Test
        @DisplayName("null input yields anyCellFound=false, not an exception")
        void handlesNull() {
            assertThat(ChatVoiceMessageTextExtractor.extract(null).anyCellFound()).isFalse();
        }

        @Test
        @DisplayName("empty input yields anyCellFound=false")
        void handlesEmptyString() {
            assertThat(ChatVoiceMessageTextExtractor.extract("").anyCellFound()).isFalse();
        }
    }
}
