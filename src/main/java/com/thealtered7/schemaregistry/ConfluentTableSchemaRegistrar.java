package com.thealtered7.schemaregistry;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import java.util.Objects;
import java.util.function.BiFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Registers Spark StructType JSON with Confluent Schema Registry by wrapping it in a JSON Schema
 * object that carries the Spark document under {@code x-spark-schema}.
 */
final class ConfluentTableSchemaRegistrar implements TableSchemaRegistrar {

    private static final Logger log = LoggerFactory.getLogger(ConfluentTableSchemaRegistrar.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final int IDENTITY_MAP_CAPACITY = 100;

    private final BiFunction<String, String, Integer> registration;

    ConfluentTableSchemaRegistrar(SchemaRegistryConfig config) {
        this(new CachedSchemaRegistryClient(
                Objects.requireNonNull(config, "config").url(), IDENTITY_MAP_CAPACITY));
    }

    ConfluentTableSchemaRegistrar(SchemaRegistryClient client) {
        this((subject, wrappedJsonSchema) -> {
            try {
                return Objects.requireNonNull(client, "client").register(subject, new JsonSchema(wrappedJsonSchema));
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    /** Visible for tests. */
    ConfluentTableSchemaRegistrar(BiFunction<String, String, Integer> registration) {
        this.registration = Objects.requireNonNull(registration, "registration");
    }

    @Override
    public String register(String subject, String sparkSchemaJson) {
        if (sparkSchemaJson == null || sparkSchemaJson.isBlank()) {
            return null;
        }
        if (subject == null || subject.isBlank()) {
            log.warn("Skipping table schema registration: subject is blank");
            return null;
        }
        try {
            String wrapped = wrapSparkSchema(sparkSchemaJson);
            Integer id = registration.apply(subject, wrapped);
            return id == null ? null : Integer.toString(id);
        } catch (Exception e) {
            log.warn(
                    "Failed to register Spark schema under subject {}; leaving schema id null",
                    subject,
                    e);
            return null;
        }
    }

    static String wrapSparkSchema(String sparkSchemaJson) throws Exception {
        JsonNode sparkSchema = MAPPER.readTree(sparkSchemaJson);
        ObjectNode wrapper = MAPPER.createObjectNode();
        wrapper.put("$schema", "http://json-schema.org/draft-07/schema#");
        wrapper.put("type", "object");
        wrapper.put("title", "spark-table-schema");
        wrapper.put("additionalProperties", true);
        wrapper.set("x-spark-schema", sparkSchema);
        return MAPPER.writeValueAsString(wrapper);
    }
}
