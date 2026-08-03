package com.thealtered7.schemaregistry;

import java.util.Objects;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Factory helpers that attach schema-registry-aware value serdes while keeping string keys.
 */
public final class SchemaAwareKafka {

    private SchemaAwareKafka() {}

    public static <T> KafkaProducer<String, T> createProducer(
            String bootstrapServers, SchemaRegistryConfig registryConfig, Class<T> valueType) {
        return createProducer(bootstrapServers, null, registryConfig, valueType);
    }

    public static <T> KafkaProducer<String, T> createProducer(
            String bootstrapServers,
            String clientId,
            SchemaRegistryConfig registryConfig,
            Class<T> valueType) {
        Objects.requireNonNull(bootstrapServers, "bootstrapServers");
        Objects.requireNonNull(registryConfig, "registryConfig");
        Objects.requireNonNull(valueType, "valueType");

        SchemaRegistrySerdeProvider provider = SchemaRegistrySerdeProviders.create(registryConfig);
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        if (clientId != null && !clientId.isBlank()) {
            props.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        }
        return new KafkaProducer<>(
                props, new StringSerializer(), provider.valueSerializer(valueType));
    }

    public static <T> KafkaConsumer<String, T> createConsumer(
            String bootstrapServers,
            String groupId,
            String clientId,
            SchemaRegistryConfig registryConfig,
            Class<T> valueType) {
        Objects.requireNonNull(bootstrapServers, "bootstrapServers");
        Objects.requireNonNull(groupId, "groupId");
        Objects.requireNonNull(registryConfig, "registryConfig");
        Objects.requireNonNull(valueType, "valueType");

        SchemaRegistrySerdeProvider provider = SchemaRegistrySerdeProviders.create(registryConfig);
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        if (clientId != null && !clientId.isBlank()) {
            props.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
        }
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return new KafkaConsumer<>(
                props, new StringDeserializer(), provider.valueDeserializer(valueType));
    }
}
