package com.thealtered7.schemaregistry;

import java.util.Objects;
import java.util.Properties;

/**
 * Configuration for Kafka value serdes backed by a schema registry (or plain JSON).
 *
 * <p>Property keys (with optional prefix, e.g. {@code app.}):
 *
 * <ul>
 *   <li>{@code schema.registry.type} — {@code none}|{@code confluent}|{@code glue} (default {@code none})
 *   <li>{@code schema.registry.url} — Confluent HTTP URL (required for {@code confluent})
 *   <li>{@code schema.registry.auto.register} — auto-register schemas (default {@code true})
 *   <li>{@code schema.registry.region} — AWS region (Glue)
 *   <li>{@code schema.registry.name} — Glue registry name
 *   <li>{@code schema.registry.compatibility} — compatibility mode hint (Glue)
 * </ul>
 */
public record SchemaRegistryConfig(
        SchemaRegistryBackend backend,
        String url,
        boolean autoRegisterSchemas,
        String region,
        String registryName,
        String compatibility) {

    public SchemaRegistryConfig {
        Objects.requireNonNull(backend, "backend");
    }

    public static SchemaRegistryConfig none() {
        return new SchemaRegistryConfig(SchemaRegistryBackend.NONE, null, false, null, null, null);
    }

    /**
     * @param properties source properties
     * @param prefix key prefix including trailing dot when non-empty (e.g. {@code "app."} or {@code ""})
     */
    public static SchemaRegistryConfig fromProperties(Properties properties, String prefix) {
        Objects.requireNonNull(properties, "properties");
        String p = prefix == null ? "" : prefix;
        SchemaRegistryBackend backend =
                SchemaRegistryBackend.fromString(properties.getProperty(p + "schema.registry.type"));
        String url = trimToNull(properties.getProperty(p + "schema.registry.url"));
        boolean autoRegister = parseBoolean(properties.getProperty(p + "schema.registry.auto.register"), true);
        String region = trimToNull(properties.getProperty(p + "schema.registry.region"));
        String registryName = trimToNull(properties.getProperty(p + "schema.registry.name"));
        String compatibility = trimToNull(properties.getProperty(p + "schema.registry.compatibility"));
        SchemaRegistryConfig config =
                new SchemaRegistryConfig(backend, url, autoRegister, region, registryName, compatibility);
        config.validate();
        return config;
    }

    public void validate() {
        if (backend == SchemaRegistryBackend.CONFLUENT) {
            if (url == null || url.isBlank()) {
                throw new IllegalArgumentException(
                        "schema.registry.url is required when schema.registry.type=confluent");
            }
        }
        if (backend == SchemaRegistryBackend.GLUE) {
            if (region == null || region.isBlank()) {
                throw new IllegalArgumentException(
                        "schema.registry.region is required when schema.registry.type=glue");
            }
            if (registryName == null || registryName.isBlank()) {
                throw new IllegalArgumentException(
                        "schema.registry.name is required when schema.registry.type=glue");
            }
        }
    }

    private static String trimToNull(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    private static boolean parseBoolean(String value, boolean defaultValue) {
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        return Boolean.parseBoolean(value.trim());
    }
}
