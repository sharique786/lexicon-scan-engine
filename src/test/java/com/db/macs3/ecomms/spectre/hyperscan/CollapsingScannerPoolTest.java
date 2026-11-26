package com.db.macs3.ecomms.spectre.hyperscan;

import com.db.macs3.ecomms.spectre.decision.FeatureScanOrchestrator;
import com.gliwka.hyperscan.wrapper.CollapsingScanner;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("CollapsingScannerPool — long-lived scanners reused across tasks, never closed")
class CollapsingScannerPoolTest {

    @Test
    @DisplayName("a returned scanner is reused by the next borrower instead of creating a new one")
    void reusesReturnedScanner() {
        CollapsingScannerPool pool = new CollapsingScannerPool(2);
        CollapsingScanner first = pool.borrow(100);
        pool.release(first);

        assertThat(pool.borrow(100)).isSameAs(first);
        assertThat(pool.createdCount()).isEqualTo(1);
    }

    @Test
    @DisplayName("scanners are keyed by their tail cap")
    void keyedByTailCap() {
        CollapsingScannerPool pool = new CollapsingScannerPool(4);
        CollapsingScanner cap100 = pool.borrow(100);
        pool.release(cap100);

        CollapsingScanner cap200 = pool.borrow(200);
        assertThat(cap200).isNotSameAs(cap100);
        assertThat(cap200.maxTailChars()).isEqualTo(200);
        pool.release(cap200);
    }

    @Test
    @DisplayName("past the cap a borrower waits until a scanner is returned")
    void waitsAtCap() throws Exception {
        CollapsingScannerPool pool = new CollapsingScannerPool(1);
        CollapsingScanner only = pool.borrow(100);

        CompletableFuture<CollapsingScanner> waiting = CompletableFuture.supplyAsync(() -> pool.borrow(100));
        Thread.sleep(200);
        assertThat(waiting).isNotDone();

        pool.release(only);
        assertThat(waiting.get(5, TimeUnit.SECONDS)).isSameAs(only);
        assertThat(pool.createdCount()).isEqualTo(1);
    }

    @Test
    @DisplayName("an orchestrator closed twice (iterator end + task listener) returns its scanner exactly once")
    void orchestratorDoubleCloseReleasesOnce() {
        CollapsingScannerPool pool = new CollapsingScannerPool(2);
        HyperscanBundleLoader loader = new HyperscanBundleLoader(Map.of(), path -> null, 1);
        FeatureScanOrchestrator orchestrator = new FeatureScanOrchestrator(loader, null, 100, pool);
        orchestrator.close();
        orchestrator.close();

        CollapsingScanner a = pool.borrow(100);
        CollapsingScanner b = pool.borrow(100);
        assertThat(a).as("a double release would hand the SAME scanner to two tasks").isNotSameAs(b);
        assertThat(pool.createdCount()).isEqualTo(2);
    }
}
