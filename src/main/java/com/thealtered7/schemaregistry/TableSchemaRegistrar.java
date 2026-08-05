package com.thealtered7.schemaregistry;

/**
 * Registers Spark/Iceberg table schemas with a schema registry and returns the registry id as a
 * string (Confluent uses integer ids; Glue may use ARNs later).
 *
 * <p>Returns {@code null} when registration is skipped or unavailable; callers should still
 * persist the Spark schema JSON on table-writes.
 */
public interface TableSchemaRegistrar {

    /**
     * @param subject registry subject (e.g. {@code iceberg.geo.public_bronze.scalars-value})
     * @param sparkSchemaJson Spark {@code StructType} JSON from {@code schema().json()}, or null
     * @return registry id string, or null if skipped/unavailable
     */
    String register(String subject, String sparkSchemaJson);
}
