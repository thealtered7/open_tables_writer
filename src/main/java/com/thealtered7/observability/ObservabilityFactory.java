package com.thealtered7.observability;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.observation.DefaultMeterObservationHandler;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.registry.otlp.OtlpConfig;
import io.micrometer.registry.otlp.OtlpMeterRegistry;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.handler.DefaultTracingObservationHandler;
import io.micrometer.tracing.otel.bridge.OtelBaggageManager;
import io.micrometer.tracing.otel.bridge.OtelCurrentTraceContext;
import io.micrometer.tracing.otel.bridge.OtelTracer;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.exporter.otlp.http.trace.OtlpHttpSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;

public final class ObservabilityFactory implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ObservabilityFactory.class);

    private static final String ENV_TRACING_ENDPOINT = "MANAGEMENT_OTLP_TRACING_ENDPOINT";
    private static final String ENV_METRICS_EXPORT_URL = "MANAGEMENT_OTLP_METRICS_EXPORT_URL";
    private static final String DEFAULT_TRACING_ENDPOINT = "http://localhost:4318/v1/traces";
    private static final String DEFAULT_METRICS_EXPORT_URL = "http://localhost:4318/v1/metrics";
    private static final String SERVICE_NAME = "open-tables-writer";

    private final OtlpMeterRegistry meterRegistry;
    private final OpenTelemetrySdk openTelemetrySdk;
    private final SdkTracerProvider tracerProvider;
    private final Observability observability;

    private ObservabilityFactory(
            OtlpMeterRegistry meterRegistry,
            OpenTelemetrySdk openTelemetrySdk,
            SdkTracerProvider tracerProvider,
            Observability observability) {
        this.meterRegistry = meterRegistry;
        this.openTelemetrySdk = openTelemetrySdk;
        this.tracerProvider = tracerProvider;
        this.observability = observability;
    }

    public static ObservabilityFactory create() {
        String tracingEndpoint = envOrDefault(ENV_TRACING_ENDPOINT, DEFAULT_TRACING_ENDPOINT);
        String metricsExportUrl = envOrDefault(ENV_METRICS_EXPORT_URL, DEFAULT_METRICS_EXPORT_URL);
        log.info("OpenTelemetry tracing endpoint: {}", tracingEndpoint);
        log.info("OpenTelemetry metrics export URL: {}", metricsExportUrl);

        OtlpMeterRegistry meterRegistry = new OtlpMeterRegistry(metricsConfig(metricsExportUrl), Clock.SYSTEM);

        OtlpHttpSpanExporter spanExporter = OtlpHttpSpanExporter.builder()
                .setEndpoint(tracingEndpoint)
                .build();

        Resource resource = Resource.getDefault()
                .merge(Resource.create(Attributes.of(AttributeKey.stringKey("service.name"), SERVICE_NAME)));

        SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
                .setResource(resource)
                .addSpanProcessor(BatchSpanProcessor.builder(spanExporter).build())
                .build();

        OpenTelemetrySdk openTelemetrySdk = OpenTelemetrySdk.builder()
                .setTracerProvider(tracerProvider)
                .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
                .build();

        OtelCurrentTraceContext traceContext = new OtelCurrentTraceContext();
        Tracer tracer = new OtelTracer(
                openTelemetrySdk.getTracer(SERVICE_NAME),
                traceContext,
                event -> {},
                new OtelBaggageManager(traceContext, Collections.emptyList(), Collections.emptyList()));

        ObservationRegistry observationRegistry = ObservationRegistry.create();
        observationRegistry.observationConfig()
                .observationHandler(new DefaultMeterObservationHandler(meterRegistry))
                .observationHandler(new DefaultTracingObservationHandler(tracer));

        Observability observability =
                new Observability(observationRegistry, meterRegistry);

        return new ObservabilityFactory(meterRegistry, openTelemetrySdk, tracerProvider, observability);
    }

    public Observability observability() {
        return observability;
    }

    public void shutdown() {
        close();
    }

    @Override
    public void close() {
        log.info("Shutting down OpenTelemetry exporters");
        meterRegistry.close();
        tracerProvider.close();
        openTelemetrySdk.close();
    }

    private static OtlpConfig metricsConfig(String metricsExportUrl) {
        Map<String, String> properties = new HashMap<>();
        properties.put("otlp.url", metricsExportUrl);
        properties.put("otlp.step", "15s");
        return new OtlpConfig() {
            @Override
            public String get(String key) {
                return properties.get(key);
            }
        };
    }

    private static String envOrDefault(String name, String defaultValue) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        return value;
    }
}
