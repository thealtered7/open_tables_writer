package com.thealtered7.schemaregistry;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

final class JacksonJsonDeserializer<T> implements Deserializer<T> {

    private final ObjectMapper mapper;
    private final Class<T> type;

    JacksonJsonDeserializer(ObjectMapper mapper, Class<T> type) {
        this.mapper = mapper;
        this.type = type;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {}

    @Override
    public T deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        try {
            return mapper.readValue(data, type);
        } catch (Exception e) {
            throw new SerializationException("Failed to deserialize value for topic " + topic, e);
        }
    }

    @Override
    public void close() {}
}
