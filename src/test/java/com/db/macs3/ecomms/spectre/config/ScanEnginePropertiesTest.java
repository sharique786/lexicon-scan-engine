package com.db.macs3.ecomms.spectre.config;

import com.gliwka.hyperscan.wrapper.CollapsingScanner;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.core.env.PropertySource;
import org.springframework.core.io.ClassPathResource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("ScanEngineProperties — every scan-engine.* key in application.yml binds")
class ScanEnginePropertiesTest {

    private static ScanEngineProperties bind(Map<String, Object> source) {
        Binder binder = new Binder(new MapConfigurationPropertySource(source));
        return binder.bindOrCreate("scan-engine", Bindable.of(ScanEngineProperties.class));
    }

    @Test
    @DisplayName("non-default values for every key are all read")
    void everyKeyBinds() {
        Map<String, Object> yml = createStringObjectMap();

        ScanEngineProperties p = bind(yml);

        assertThat(p.getMaxCachedDatabasesPerPartition()).isEqualTo(7);
        assertThat(p.isSharedBundleCacheEnabled()).isFalse();
        assertThat(p.getMaxSharedIdleBundlesPerExecutor()).isEqualTo(5);
        assertThat(p.isSortPartitionsByFeatureSet()).isFalse();
        assertThat(p.isPartitionByFeatureSet()).isTrue();
        assertThat(p.getMaxTailChars()).isEqualTo(250);
        assertThat(p.getCreatedBy()).isEqualTo("someone");
    }

    private static @NonNull Map<String, Object> createStringObjectMap() {
        Map<String, Object> yml = new HashMap<>();
        yml.put("scan-engine.max-cached-databases-per-partition", "7");
        yml.put("scan-engine.shared-bundle-cache-enabled", "false");
        yml.put("scan-engine.max-shared-idle-bundles-per-executor", "5");
        yml.put("scan-engine.sort-partitions-by-feature-set", "false");
        yml.put("scan-engine.partition-by-feature-set", "true");
        yml.put("scan-engine.max-tail-chars", "250");
        yml.put("scan-engine.created-by", "someone");
        return yml;
    }

    @Test
    @DisplayName("absent keys keep their defaults")
    void defaults() {
        ScanEngineProperties p = bind(Map.of());

        assertThat(p.getMaxCachedDatabasesPerPartition()).isEqualTo(20);
        assertThat(p.isSharedBundleCacheEnabled()).isTrue();
        assertThat(p.getMaxSharedIdleBundlesPerExecutor()).isEqualTo(32);
        assertThat(p.isSortPartitionsByFeatureSet()).isTrue();
        assertThat(p.isPartitionByFeatureSet()).isFalse();
        assertThat(p.getMaxTailChars()).isEqualTo(CollapsingScanner.DEFAULT_MAX_TAIL_CHARS);
        assertThat(p.getCreatedBy()).isEqualTo("SPECTRE-COMPOSER-SA");
    }

    @Test
    @DisplayName("the real application.yml: every scan-engine key maps to a property")
    void realApplicationYml() throws Exception {
        List<PropertySource<?>> sources =
                new YamlPropertySourceLoader().load("application", new ClassPathResource("application.yml"));
        Map<String, Object> flat = new HashMap<>();
        for (PropertySource<?> s : sources) {
            if (s.getSource() instanceof Map<?, ?> m) {
                m.forEach((k, v) -> flat.put(k.toString(), v.toString()));
            }
        }
        List<String> keys = flat.keySet().stream().filter(k -> k.startsWith("scan-engine.")).toList();
        assertThat(keys).isNotEmpty();

        ScanEngineProperties p = bind(flat);
        // Each yml value must equal what the matching getter returns, so a key with no setter (silently ignored by
        // the binder) fails here.
        for (String key : keys) {
            assertThat(String.valueOf(valueOf(p, key.substring("scan-engine.".length()))))
                    .as(key).isEqualTo(flat.get(key));
        }
    }

    private static Object valueOf(ScanEngineProperties p, String key) {
        return switch (key) {
            case "max-cached-databases-per-partition" -> p.getMaxCachedDatabasesPerPartition();
            case "shared-bundle-cache-enabled" -> p.isSharedBundleCacheEnabled();
            case "max-shared-idle-bundles-per-executor" -> p.getMaxSharedIdleBundlesPerExecutor();
            case "sort-partitions-by-feature-set" -> p.isSortPartitionsByFeatureSet();
            case "partition-by-feature-set" -> p.isPartitionByFeatureSet();
            case "max-tail-chars" -> p.getMaxTailChars();
            case "created-by" -> p.getCreatedBy();
            default -> throw new AssertionError("application.yml key scan-engine." + key
                    + " has no ScanEngineProperties property");
        };
    }
}
