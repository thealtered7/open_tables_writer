package com.thealtered7.observability;

import java.util.Collections;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.observation.ObservationRegistry;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

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
}
