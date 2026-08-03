package com.thealtered7.schemaregistry;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

final class NoneSchemaRegistrySerdeProvider implements SchemaRegistrySerdeProvider {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    @Override
    public SchemaRegistryBackend backend() {
        return SchemaRegistryBackend.NONE;
    }

    @Override
    public <T> Serializer<T> valueSerializer(Class<T> type) {
        return new JacksonJsonSerializer<>(MAPPER);
    }

    @Override
    public <T> Deserializer<T> valueDeserializer(Class<T> type) {
        return new JacksonJsonDeserializer<>(MAPPER, type);
    }
}
