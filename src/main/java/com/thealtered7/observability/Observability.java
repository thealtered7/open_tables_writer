package com.thealtered7.observability;

import java.util.Collections;
import java.util.Map;
import java.util.function.Supplier;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;

public class Observability {

    @FunctionalInterface
    public interface ThrowingSupplier<T> {
        T get() throws Exception;
    }

    public static final String PREFIX = "table_writer_kafka";
    public static final String DELTA_TABLE_WRITER_PREFIX = "delta_table_writer";
    public static final String ICEBERG_TABLE_WRITER_PREFIX = "iceberg_table_writer";
    public static final String DEBEZIUM_PAYLOAD_FLATTENER_PREFIX = "debezium_payload_flattener";
    public static final String TYPE2_DIMENSION_KAFKA_PREFIX = "type2_dimension_kafka";
    public static final String TYPE2_DIMENSION_TRANSFORMER_PREFIX = "type2_dimension_transformer";
    public static final String DATAPIPELINES_CLIENT_PREFIX = "datapipelines_client";

    private final ObservationRegistry observationRegistry;
    private final MeterRegistry meterRegistry;

    public Observability(ObservationRegistry observationRegistry, MeterRegistry meterRegistry) {
        this.observationRegistry = observationRegistry;
        this.meterRegistry = meterRegistry;
    }

    public static String spanName(String prefix, String operation) {
        return prefix + "." + operation;
    }

    public static String metricName(String prefix, String operation) {
        return prefix + "." + operation + ".records";
    }

    public static String spanName(String operation) {
        return spanName(PREFIX, operation);
    }

    public static String metricName(String operation) {
        return metricName(PREFIX, operation);
    }

    public static Observability noop() {
        return new Observability(ObservationRegistry.create(), new SimpleMeterRegistry());
    }

    public String observeOperation(String prefix, String operation, Map<String, String> tags, Supplier<String> action) {
        String outcome = "error";
        Observation observation = Observation.createNotStarted(spanName(prefix, operation), observationRegistry);
        if (tags != null) {
            tags.forEach(observation::lowCardinalityKeyValue);
        }
        observation.start();
        try (Observation.Scope scope = observation.openScope()) {
            outcome = action.get();
            return outcome;
        } catch (RuntimeException ex) {
            observation.error(ex);
            throw ex;
        } finally {
            observation.lowCardinalityKeyValue("outcome", outcome);
            meterRegistry.counter(metricName(prefix, operation), "outcome", outcome).increment();
            observation.stop();
        }
    }

    public void observeOperationVoid(String prefix, String operation, Map<String, String> tags, Supplier<String> action) {
        observeOperation(prefix, operation, tags, action);
    }

    public void observeOperationVoid(String prefix, String operation, Supplier<String> action) {
        observeOperationVoid(prefix, operation, Collections.emptyMap(), action);
    }

    public <T> T observeCallable(String prefix, String operation, Map<String, String> tags, ThrowingSupplier<T> action)
            throws Exception {
        String outcome = "error";
        Observation observation = Observation.createNotStarted(spanName(prefix, operation), observationRegistry);
        if (tags != null) {
            tags.forEach(observation::lowCardinalityKeyValue);
        }
        observation.start();
        try (Observation.Scope scope = observation.openScope()) {
            T result = action.get();
            outcome = "success";
            return result;
        } catch (Exception ex) {
            observation.error(ex);
            throw ex;
        } finally {
            observation.lowCardinalityKeyValue("outcome", outcome);
            meterRegistry.counter(metricName(prefix, operation), "outcome", outcome).increment();
            observation.stop();
        }
    }

    public void observeCallableVoid(String prefix, String operation, Map<String, String> tags, ThrowingSupplier<String> action)
            throws Exception {
        observeCallable(prefix, operation, tags, () -> {
            action.get();
            return null;
        });
    }

    public String observeAccept(String operation, Map<String, String> tags, Supplier<String> action) {
        return observeOperation(PREFIX, operation, tags, action);
    }

    public void observeAcceptVoid(String operation, Map<String, String> tags, Supplier<String> action) {
        observeOperationVoid(PREFIX, operation, tags, action);
    }

    public void observeAcceptVoid(String operation, Supplier<String> action) {
        observeAcceptVoid(operation, Collections.emptyMap(), action);
    }

    public void lowCardinalityTag(String key, String value) {
        Observation current = observationRegistry.getCurrentObservation();
        if (current != null) {
            current.lowCardinalityKeyValue(key, value);
        }
    }
}
