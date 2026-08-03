package com.thealtered7.schemaregistry;

/**
 * Supported schema registry backends. Confluent and Glue use incompatible Kafka wire formats,
 * so producer and consumer must use the same backend.
 */
public enum SchemaRegistryBackend {
    /** Plain Jackson JSON bytes with no registry (default for tests / opt-out). */
    NONE,
    /** Confluent Schema Registry over HTTP (dev / docker-compose). */
    CONFLUENT,
    /** AWS Glue Schema Registry (production target; provider not yet implemented). */
    GLUE;

    public static SchemaRegistryBackend fromString(String value) {
        if (value == null || value.isBlank()) {
            return NONE;
        }
        return SchemaRegistryBackend.valueOf(value.trim().toUpperCase());
    }
}
