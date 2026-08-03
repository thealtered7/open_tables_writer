package com.thealtered7.schemaregistry;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Pluggable Kafka value serdes for a schema registry backend. Swap implementations via
 * {@code schema.registry.type} without changing producer/consumer call sites.
 */
public interface SchemaRegistrySerdeProvider {

    SchemaRegistryBackend backend();

    <T> Serializer<T> valueSerializer(Class<T> type);

    <T> Deserializer<T> valueDeserializer(Class<T> type);
}
