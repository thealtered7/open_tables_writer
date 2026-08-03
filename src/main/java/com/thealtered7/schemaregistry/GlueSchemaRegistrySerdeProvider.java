package com.thealtered7.schemaregistry;

import java.util.Objects;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Placeholder for AWS Glue Schema Registry. Wire format differs from Confluent; implement with
 * {@code software.amazon.glue:schema-registry-serde} (or equivalent) when enabling production.
 */
final class GlueSchemaRegistrySerdeProvider implements SchemaRegistrySerdeProvider {

    private final SchemaRegistryConfig config;

    GlueSchemaRegistrySerdeProvider(SchemaRegistryConfig config) {
        this.config = Objects.requireNonNull(config, "config");
    }

    @Override
    public SchemaRegistryBackend backend() {
        return SchemaRegistryBackend.GLUE;
    }

    @Override
    public <T> Serializer<T> valueSerializer(Class<T> type) {
        throw unsupported();
    }

    @Override
    public <T> Deserializer<T> valueDeserializer(Class<T> type) {
        throw unsupported();
    }

    private UnsupportedOperationException unsupported() {
        return new UnsupportedOperationException(
                "schema.registry.type=glue is not implemented yet. Configured region="
                        + config.region()
                        + ", registryName="
                        + config.registryName()
                        + ", compatibility="
                        + config.compatibility()
                        + ". Add a GlueSchemaRegistrySerdeProvider backed by the AWS Glue Schema Registry "
                        + "Kafka serde and keep producer/consumer on the same backend.");
    }
}
