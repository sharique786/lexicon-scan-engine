package com.db.macs3.ecomms.spectre.spark;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.RuntimeConfig;
import org.apache.spark.sql.SparkSession;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Spring-managed factory for this job's single driver-side {@link SparkSession}
 * and {@link JavaSparkContext} — split out of {@link ScanEngineJobRunner} so
 * that class can take both as constructor-injected dependencies rather than
 * constructing them itself. Both beans are ordinary Spring singletons, which
 * matches Spark's own assumption of exactly one {@code SparkSession}/
 * {@code SparkContext} per driver JVM — there is only ever one Spring
 * {@code ApplicationContext} per driver too (see
 * {@link com.db.macs3.ecomms.spectre.LexiconScanEngineApplication} class
 * Javadoc, "Spring's reach ends at the driver").
 */
@Configuration
public class SparkSessionConfig {

    /**
     * {@code spark.serializer} is a "static" config — only takes effect if set
     * before the {@code SparkContext} is actually constructed, via
     * {@code SparkSession.builder().config(...)}, never via
     * {@code spark.conf().set(...)} afterward — so it is set here, on the
     * builder, before {@link #applyJobSpecificSparkConf} runs any further
     * (runtime-safe) config against the session it returns.
     */
    @Bean
    public SparkSession sparkSession() {
        SparkSession spark = SparkSession.builder()
                .appName("spectre-lexicon-tagging-engine")
                .config(SparkConfigKeys.SERIALIZER, "org.apache.spark.serializer.KryoSerializer")
                .getOrCreate();
        applyJobSpecificSparkConf(spark);
        return spark;
    }

    /**
     * Broadcast via {@code JavaSparkContext} (not the raw Scala
     * {@code SparkContext}, which requires an implicit {@code ClassTag} that
     * Java code cannot supply naturally) — the standard Java-side way to
     * create a {@code Broadcast}; see {@link ScanEngineJobRunner runPipeline()}.
     */
    @Bean
    public JavaSparkContext javaSparkContext(SparkSession sparkSession) {
        return JavaSparkContext.fromSparkContext(sparkSession.sparkContext());
    }

    /**
     * Sets this job's OWN critical performance/skew-handling configs
     * explicitly, rather than trusting the shared Dataproc cluster's
     * {@code spark-defaults.conf} to already suit this specific workload —
     * see README "Performance & scalability" for the full reasoning. On a
     * cluster shared with other tenants, global defaults reflect whatever
     * mix of OTHER jobs has driven them, not this job's own two independent
     * skew sources (a single message's attachment text can be far larger
     * than the median; a single message's applicable-lexicon-feature count
     * can also be far larger than the median, independently of attachment
     * size) — every setting below is a per-{@code SparkSession} RUNTIME
     * config, safe to set here (unlike {@code spark.serializer}, a "static"
     * config that must be set on the builder before {@code getOrCreate()} —
     * see {@link #sparkSession}).
     */
    private void applyJobSpecificSparkConf(SparkSession spark) {
        RuntimeConfig conf = spark.conf();

        // AQE + skew-join splitting: on by default since Spark 3.2, but never assumed here —
        // this job's correctness/performance under skew depends on it, so it is asserted
        // explicitly rather than hoped for.
        conf.set(SparkConfigKeys.ADAPTIVE_ENABLED, "true");
        conf.set(SparkConfigKeys.ADAPTIVE_COALESCE_PARTITIONS_ENABLED, "true");
        conf.set(SparkConfigKeys.ADAPTIVE_SKEW_JOIN_ENABLED, "true");
        // Tightened from Spark's own defaults (factor 5, 256MB threshold): a single message
        // with an unusually large attachment can dwarf the median shuffle-partition size by
        // far more than 5x while still being one real, unsplittable row — the default
        // threshold can under-react to exactly this job's specific skew shape. Reasoned
        // defaults, treated as a starting point to monitor and adjust against real production
        // message-size distributions.
        conf.set(SparkConfigKeys.ADAPTIVE_SKEW_JOIN_SKEWED_PARTITION_FACTOR, "3");
        conf.set(SparkConfigKeys.ADAPTIVE_SKEW_JOIN_SKEWED_PARTITION_THRESHOLD_BYTES, "128m");
        conf.set(SparkConfigKeys.ADAPTIVE_ADVISORY_PARTITION_SIZE_BYTES, "64m");

        // spark.sql.shuffle.partitions: Spark's own hardcoded default (200) has no relationship
        // to how many executor cores THIS run actually has on a shared, dynamically-allocated
        // cluster — computed here relative to the driver's own view of available parallelism at
        // job start instead, floored at 200 so a slow dynamic-allocation ramp-up at startup
        // never produces an under-parallelised shuffle. AQE's coalescePartitions still merges
        // this back down post-shuffle as actual data volume allows.
        int defaultParallelism = spark.sparkContext().defaultParallelism();
        conf.set(SparkConfigKeys.SHUFFLE_PARTITIONS, String.valueOf(Math.max(200, defaultParallelism * 3)));

        // Smaller AVRO read-side partitions: the default 128MB max-partition-bytes groups
        // messages into a read partition purely by source-file byte range, with no awareness
        // that one of those bytes might belong to a single message's giant attachment — a
        // smaller ceiling here means fewer OTHER messages get bundled alongside a large one
        // into the same initial partition, reducing (not eliminating — a single giant record
        // is still a single giant record) the odds that one partition's read+decode cost
        // dominates the whole stage's wall-clock time before AQE's post-shuffle rebalancing
        // even has a chance to help.
        conf.set(SparkConfigKeys.FILES_MAX_PARTITION_BYTES, "67108864"); // 64MB
    }
}
