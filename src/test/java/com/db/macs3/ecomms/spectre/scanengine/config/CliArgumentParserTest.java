package com.db.macs3.ecomms.spectre.scanengine.config;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("CliArgumentParser")
class CliArgumentParserTest {

    @Test
    @DisplayName("parses a full set of --key=value arguments in order")
    void parsesMultipleArguments() {
        Map<String, String> parsed = CliArgumentParser.parse(new String[]{
                "--process_id=p-1",
                "--pipeline_exec_id=pe-1",
                "--trigger_type=policy-alert-live",
        });

        assertThat(parsed).containsExactly(
                Map.entry("process_id", "p-1"),
                Map.entry("pipeline_exec_id", "pe-1"),
                Map.entry("trigger_type", "policy-alert-live"));
    }

    @Test
    @DisplayName("splits only on the FIRST '=' — a value containing '=' is kept intact")
    void keepsEqualsSignsInsideTheValue() {
        Map<String, String> parsed = CliArgumentParser.parse(new String[]{"--query=a=b&c=d"});
        assertThat(parsed).containsEntry("query", "a=b&c=d");
    }

    @Test
    @DisplayName("an empty args array parses to an empty map")
    void parsesEmptyArgsArray() {
        assertThat(CliArgumentParser.parse(new String[0])).isEmpty();
    }

    @Test
    @DisplayName("rejects an argument without the leading --")
    void rejectsArgumentWithoutPrefix() {
        assertThatThrownBy(() -> CliArgumentParser.parse(new String[]{"process_id=p-1"}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("process_id=p-1");
    }

    @Test
    @DisplayName("rejects an argument with no '=' at all")
    void rejectsArgumentWithoutEquals() {
        assertThatThrownBy(() -> CliArgumentParser.parse(new String[]{"--just-a-flag"}))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("rejects a null argument")
    void rejectsNullArgument() {
        assertThatThrownBy(() -> CliArgumentParser.parse(new String[]{null}))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("require returns the value when present and non-blank")
    void requireReturnsPresentValue() {
        Map<String, String> args = Map.of("policy_engine_id", "101");
        assertThat(CliArgumentParser.require(args, "policy_engine_id")).isEqualTo("101");
    }

    @Test
    @DisplayName("require throws when the key is absent")
    void requireThrowsWhenAbsent() {
        assertThatThrownBy(() -> CliArgumentParser.require(Map.of(), "policy_engine_id"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("policy_engine_id");
    }

    @Test
    @DisplayName("require throws when the value is blank")
    void requireThrowsWhenBlank() {
        Map<String, String> args = Map.of("policy_engine_id", "   ");
        assertThatThrownBy(() -> CliArgumentParser.require(args, "policy_engine_id"))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
