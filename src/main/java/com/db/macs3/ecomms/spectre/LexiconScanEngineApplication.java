package com.db.macs3.ecomms.spectre;

import com.db.macs3.ecomms.spectre.engine.LexiconScanEngine;
import com.db.macs3.ecomms.spectre.model.JobConfig;
import com.db.macs3.ecomms.spectre.model.ScanEngineArgs;
import com.db.macs3.ecomms.spectre.reader.JobConfigReader;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * Entry point for the Lexicon Scan Engine Spark Dataproc job.
 *
 * <h2>Invocation</h2>
 * <pre>
 * gcloud dataproc jobs submit spark \
 *   --cluster=spectre-cluster --region=us-central1 \
 *   --jar=gs://bucket/jars/lexicon-scan-engine-2.0.0.jar \
 *   --properties='spark.executor.memory=8g,spark.driver.memory=4g' \
 *   -- \
 *   --processId        "1234-5678-9810-1234" \
 *   --pipelineExecId   "1234-5678-9810-1230" \
 *   --policyEngineId   "1" \
 *   --triggerType      "policy-alert-live" \
 *   --runDate          "20260713" \
 *   --configGcsPath    "gs://spectre-config-bucket/scan-engine/prod.json" \
 *   --compsrDagName    "spectre-lexicon-tagging" \
 *   --compsrDagPath    "gs://spectre-dags/spectre-lexicon-tagging" \
 *   --dprocScriptName  "lexicon-scan-engine-2.0.0.jar" \
 *   --dprocScriptPath  "gs://bucket/jars/lexicon-scan-engine-2.0.0.jar"
 * </pre>
 *
 * <h2>Table/view names live in a GCS JSON file, not on the command line</h2>
 * <p>{@code --configGcsPath} points at a {@link JobConfig} JSON file — see its
 * Javadoc for the full structure. This keeps the Airflow DAG task definition
 * stable across dev/test/prod environments; only the config file path changes.
 */
@SpringBootApplication
public class LexiconScanEngineApplication {

    private static final Logger log = LoggerFactory.getLogger(LexiconScanEngineApplication.class);

    public static void main(String[] args) {
        log.info("=== Lexicon Scan Engine Starting ===");

        ScanEngineArgs scanEngineArgs;
        try {
            scanEngineArgs = ScanEngineArgs.parse(args);
            log.info("Parsed runtime args: {}", scanEngineArgs);
        } catch (IllegalArgumentException e) {
            log.error("Failed to parse runtime arguments: {}", e.getMessage());
            System.exit(1);
            return;
        }

        ConfigurableApplicationContext ctx = null;
        try {
            SpringApplication app = new SpringApplication(LexiconScanEngineApplication.class);
            ctx = app.run(args);

            JobConfigReader configReader = ctx.getBean(JobConfigReader.class);
            JobConfig jobConfig = configReader.load(scanEngineArgs.getConfigGcsPath());

            SparkSession       spark  = ctx.getBean(SparkSession.class);
            LexiconScanEngine  engine = ctx.getBean(LexiconScanEngine.class);

            log.info("=== Pipeline execution starting ===");
            engine.run(spark, jobConfig, scanEngineArgs);
            log.info("=== Pipeline execution complete ===");

        } catch (Exception e) {
            log.error("Lexicon Scan Engine failed with unexpected error: {}", e.getMessage(), e);
            System.exit(2);
        } finally {
            if (ctx != null) {
                ctx.close();
            }
        }

        log.info("=== Lexicon Scan Engine Finished ===");
        System.exit(0);
    }
}
