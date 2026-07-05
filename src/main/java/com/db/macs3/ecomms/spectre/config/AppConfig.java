package com.db.macs3.ecomms.spectre.config;

import com.db.macs3.ecomms.spectre.reader.AvroMessageReader;
import com.db.macs3.ecomms.spectre.reader.JsonMessageReader;
import com.db.macs3.ecomms.spectre.reader.MessageReader;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

/**
 * Spring {@code @Configuration} wiring up Spark infrastructure and selecting
 * the correct {@link MessageReader} implementation for the active profile.
 *
 * <h2>SparkSession</h2>
 * <p>Production ({@code @Profile("!test")}) acquires the Dataproc cluster's
 * existing SparkContext via {@code getOrCreate()}. Tests
 * ({@code @Profile("test")}) get a local {@code local[*]} session — no
 * cluster required, works identically on a developer laptop, Windows, macOS,
 * or a GitHub Actions runner.
 *
 * <h2>MessageReader selection</h2>
 * <p>Production wires {@link AvroMessageReader} (reads from GCS). The
 * {@code test} profile wires {@link JsonMessageReader} (reads local/classpath
 * JSON fixtures — see {@code application-test.yml} for the
 * {@code app.lexicon.test-message-source} property pointing at fixture files).
 * Both beans are exposed under the qualifier {@code "messageReader"} so
 * {@link com.db.macs3.ecomms.spectre.engine.LexiconScanEngine} depends only
 * on the {@link MessageReader} interface, never on a concrete implementation.
 */
@Configuration
public class AppConfig {

    private static final Logger log = LoggerFactory.getLogger(AppConfig.class);

    @Value("${spark.app.name:LexiconScanEngine}")
    private String appName;

    @Value("${spark.master:#{null}}")
    private String sparkMaster;

    @Value("${spark.sql.shuffle.partitions:200}")
    private String shufflePartitions;

    @Value("${spark.default.parallelism:200}")
    private String defaultParallelism;

    // ── SparkSession beans ────────────────────────────────────────────────────

    @Bean
    @Profile("!test")
    public SparkSession sparkSession() {
        log.info("Creating SparkSession for production (Dataproc cluster)");
        SparkSession session = SparkSession.builder()
                .appName(appName)
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                .config("spark.sql.adaptive.skewJoin.enabled", "true")
                .config("spark.sql.shuffle.partitions", shufflePartitions)
                .config("spark.default.parallelism", defaultParallelism)
                .config("spark.datasource.bigquery.writeMethod", "STORAGE_WRITE_API")
                .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
                .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
                .getOrCreate();
        log.info("SparkSession acquired: version={}, master={}", session.version(), session.sparkContext().master());
        return session;
    }

    @Bean
    @Profile("test")
    public SparkSession testSparkSession() {
        log.info("Creating local SparkSession for tests");
        String master = sparkMaster != null ? sparkMaster : "local[2]";
        return SparkSession.builder()
                .appName(appName + "-test")
                .master(master)
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .config("spark.sql.shuffle.partitions", "4")
                .config("spark.default.parallelism", "4")
                .config("spark.ui.enabled", "false")
                // Avoid platform-specific Hadoop native library warnings on Windows CI runners.
                .config("spark.hadoop.io.nativeio.enabled", "false")
                .getOrCreate();
    }

    // ── MessageReader beans ───────────────────────────────────────────────────

    @Bean("messageReader")
    @Profile("!test")
    public MessageReader avroMessageReaderBean(AvroMessageReader reader) {
        log.info("Wiring production MessageReader: AvroMessageReader");
        return reader;
    }

    @Bean("messageReader")
    @Profile("test")
    public MessageReader jsonMessageReaderBean(JsonMessageReader reader) {
        log.info("Wiring test MessageReader: JsonMessageReader");
        return reader;
    }
}
