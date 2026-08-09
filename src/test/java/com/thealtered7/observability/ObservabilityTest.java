package com.thealtered7.observability;

import java.util.Collections;

import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.observation.ObservationRegistry;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ObservabilityTest {

    @Test
    void noopCompletesWithoutError() {
        Observability observability = Observability.noop();
        assertDoesNotThrow(() -> observability.observeOperationVoid(
                Observability.PREFIX, "test_operation", () -> "success"));
    }

    @Test
    void observeOperationIncrementsSuccessCounter() {
        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
        ObservationRegistry observationRegistry = ObservationRegistry.create();
        Observability observability = new Observability(observationRegistry, meterRegistry);

        String outcome = observability.observeOperation(
                Observability.PREFIX, "test_operation", Collections.emptyMap(), () -> "success");

        assertEquals("success", outcome);
        assertEquals(
                1.0,
                meterRegistry
                        .counter(Observability.metricName(Observability.PREFIX, "test_operation"), "outcome", "success")
                        .count());
    }

    @Test
    void bindJvmMetricsRegistersMemoryAndCpuMeters() {
        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
        try (JvmGcMetrics ignored = ObservabilityFactory.bindJvmMetrics(meterRegistry)) {
            assertFalse(meterRegistry.find("jvm.memory.used").gauges().isEmpty());
            assertTrue(
                    meterRegistry.find("process.cpu.usage").gauge() != null
                            || meterRegistry.find("system.cpu.usage").gauge() != null);
        }
    }
}
