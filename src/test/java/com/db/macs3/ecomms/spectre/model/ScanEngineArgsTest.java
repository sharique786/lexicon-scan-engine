package com.db.macs3.ecomms.spectre.model;

import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.*;

@DisplayName("ScanEngineArgs Tests")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ScanEngineArgsTest {

    private String[] fullValidArgs() {
        return new String[]{
                "--processId",        "proc-001",
                "--pipelineExecId",   "exec-002",
                "--policyEngineId",   "pol-003",
                "--triggerType",      "policy-alert-live",
                "--evalTestId",       "101",
                "--runDate",          "20260713",
                "--configGcsPath",    "gs://bucket/config/prod.json",
                "--compsrDagName",    "spectre-lexicon-tagging",
                "--compsrDagPath",    "gs://dags/spectre-lexicon-tagging",
                "--dprocScriptName",  "lexicon-scan-engine.jar",
                "--dprocScriptPath",  "gs://jars/lexicon-scan-engine.jar"
        };
    }

    @Test @Order(1)
    @DisplayName("parse() populates every field correctly")
    void parseAllFields() {
        ScanEngineArgs args = ScanEngineArgs.parse(fullValidArgs());

        assertThat(args.getProcessId()).isEqualTo("proc-001");
        assertThat(args.getPipelineExecId()).isEqualTo("exec-002");
        assertThat(args.getPolicyEngineId()).isEqualTo("pol-003");
        assertThat(args.getTriggerType()).isEqualTo("policy-alert-live");
        assertThat(args.getEvalTestId()).isEqualTo("101");
        assertThat(args.getRunDate()).isEqualTo("20260713");
        assertThat(args.getConfigGcsPath()).isEqualTo("gs://bucket/config/prod.json");
        assertThat(args.getCompsrDagName()).isEqualTo("spectre-lexicon-tagging");
        assertThat(args.getCompsrDagPath()).isEqualTo("gs://dags/spectre-lexicon-tagging");
        assertThat(args.getDprocScriptName()).isEqualTo("lexicon-scan-engine.jar");
        assertThat(args.getDprocScriptPath()).isEqualTo("gs://jars/lexicon-scan-engine.jar");
    }

    @Test @Order(2)
    @DisplayName("parse() ignores unknown flags")
    void parseIgnoresUnknownFlags() {
        List<String> argList = new ArrayList<>(Arrays.asList(fullValidArgs()));
        argList.add("--unknownFlag");
        argList.add("value");

        ScanEngineArgs args = ScanEngineArgs.parse(argList.toArray(new String[0]));
        assertThat(args.getProcessId()).isEqualTo("proc-001");
    }

    @Test @Order(3)
    @DisplayName("parse() succeeds without optional evalTestId/compsr*/dproc* fields")
    void parseSucceedsWithoutOptionalFields() {
        ScanEngineArgs args = ScanEngineArgs.parse(new String[]{
                "--processId",      "p1",
                "--pipelineExecId", "e1",
                "--policyEngineId", "po1",
                "--triggerType",    "policy-alert-test",
                "--runDate",        "20260101",
                "--configGcsPath",  "gs://bucket/config.json"
        });
        assertThat(args.getProcessId()).isEqualTo("p1");
        assertThat(args.getEvalTestId()).isNull();
        assertThat(args.getCompsrDagName()).isNull();
    }

    @ParameterizedTest(name = "Missing required arg: --{0}")
    @CsvSource({"processId", "pipelineExecId", "policyEngineId", "triggerType", "runDate", "configGcsPath"})
    @Order(10)
    @DisplayName("parse() throws IllegalArgumentException when a required argument is missing")
    void parseThrowsOnMissingRequired(String missingArg) {
        List<String> argList = new ArrayList<>(Arrays.asList(fullValidArgs()));
        String flag = "--" + missingArg;
        int idx = argList.indexOf(flag);
        if (idx >= 0) {
            argList.remove(idx + 1);
            argList.remove(idx);
        }
        assertThatThrownBy(() -> ScanEngineArgs.parse(argList.toArray(new String[0])))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(missingArg);
    }

    @Test @Order(20)
    @DisplayName("STAGE_NAME constant matches platform convention")
    void stageNameConstant() {
        assertThat(ScanEngineArgs.STAGE_NAME).isEqualTo("spectre-lexicon-tagging");
    }
}
