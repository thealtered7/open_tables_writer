package com.thealtered7.schemaregistry;

import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializerConfig;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Confluent Schema Registry (HTTP) using JSON Schema wire format.
 */
final class ConfluentSchemaRegistrySerdeProvider implements SchemaRegistrySerdeProvider {

    private final SchemaRegistryConfig config;

    ConfluentSchemaRegistrySerdeProvider(SchemaRegistryConfig config) {
        this.config = Objects.requireNonNull(config, "config");
    }

    @Override
    public SchemaRegistryBackend backend() {
        return SchemaRegistryBackend.CONFLUENT;
    }

    @Override
    public <T> Serializer<T> valueSerializer(Class<T> type) {
        KafkaJsonSchemaSerializer<T> serializer = new KafkaJsonSchemaSerializer<>();
        serializer.configure(baseConfigs(), false);
        return serializer;
    }

    @Override
    public <T> Deserializer<T> valueDeserializer(Class<T> type) {
        KafkaJsonSchemaDeserializer<T> deserializer = new KafkaJsonSchemaDeserializer<>();
        Map<String, Object> configs = baseConfigs();
        configs.put(KafkaJsonSchemaDeserializerConfig.JSON_VALUE_TYPE, type.getName());
        deserializer.configure(configs, false);
        return deserializer;
    }

    private Map<String, Object> baseConfigs() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, config.url());
        configs.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, config.autoRegisterSchemas());
        return configs;
    }
}
