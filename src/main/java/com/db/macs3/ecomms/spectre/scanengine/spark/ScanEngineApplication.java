package com.db.macs3.ecomms.spectre.scanengine.spark;

import com.db.macs3.ecomms.spectre.scanengine.config.ScanEngineProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * Real Spring Boot bootstrap for the Lexicon Scan Engine's DRIVER process.
 *
 * <h2>Spring's reach ends at the driver — this is a deliberate, not
 * accidental, architectural boundary</h2>
 * <p>A Spark job's driver and its executors are separate JVMs; executors
 * run whatever closures Spark serialises and ships to them (see
 * {@code PartitionProcessor}), with no Spring {@code ApplicationContext} of
 * their own — Spring's dependency-injection container fundamentally cannot
 * span a Spark cluster. Every class this application manages via Spring
 * (see {@link ScanEngineJobRunner}, {@code GcsClient} as used from the
 * driver) therefore runs ONLY on the driver: reading configuration,
 * resolving paths, orchestrating reads/writes, and building the Spark
 * {@code Dataset} pipeline. Classes that must also run inside executor-side
 * closures ({@code HyperscanBundleLoader}, {@code FeatureScanOrchestrator},
 * {@code DecisionTreeEvaluator}, {@code FeatureGroupingService},
 * {@code OutputRowBuilder}, and the various static utility classes) are
 * deliberately plain, Spring-independent Java — they must be constructible
 * and Java-serialisable with no ApplicationContext available, since that is
 * exactly the environment they run in once Spark ships them to an executor.
 * Retrofitting these into Spring beans would not add anything Spring can
 * actually provide there and would risk making a class accidentally
 * non-serialisable (a live Spring proxy/context reference is not
 * meaningfully serialisable across a cluster).
 */
@SpringBootApplication
@EnableConfigurationProperties(ScanEngineProperties.class)
public class ScanEngineApplication {

    /**
     * @param args the 7 {@code --key=value} Dataproc submit arguments Composer
     *              supplies (see {@code RuntimeArgs} class Javadoc for the full
     *              list, including {@code --config_file_path}, a GCS path to a
     *              {@code DataprocConfig} YAML file). Spring Boot's own
     *              argument parsing is bypassed for these (they look like
     *              {@code --key=value} but are job arguments, not
     *              {@code --spring.*} style properties) — they are read
     *              directly from {@code args} after the Spring context starts.
     */
    public static void main(String[] args) throws Exception {
        ConfigurableApplicationContext context = SpringApplication.run(ScanEngineApplication.class, args);

        try (context) {
            ScanEngineJobRunner runner = context.getBean(ScanEngineJobRunner.class);
            runner.run(args);
        }
    }
}
