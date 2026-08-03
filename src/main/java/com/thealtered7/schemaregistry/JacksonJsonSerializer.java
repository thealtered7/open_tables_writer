package com.thealtered7.schemaregistry;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

final class JacksonJsonSerializer<T> implements Serializer<T> {

    private final ObjectMapper mapper;

    JacksonJsonSerializer(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {}

    @Override
    public byte[] serialize(String topic, T data) {
        if (data == null) {
            return null;
        }
        try {
            return mapper.writeValueAsBytes(data);
        } catch (Exception e) {
            throw new SerializationException("Failed to serialize value for topic " + topic, e);
        }
    }

    @Override
    public void close() {}
}
