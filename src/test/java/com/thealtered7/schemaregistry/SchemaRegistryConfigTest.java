package com.thealtered7.schemaregistry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Properties;
import org.junit.jupiter.api.Test;

class SchemaRegistryConfigTest {

    @Test
    void defaultsToNoneWhenTypeMissing() {
        SchemaRegistryConfig config = SchemaRegistryConfig.fromProperties(new Properties(), "");
        assertEquals(SchemaRegistryBackend.NONE, config.backend());
    }

    @Test
    void readsConfluentConfig() {
        Properties props = new Properties();
        props.setProperty("schema.registry.type", "confluent");
        props.setProperty("schema.registry.url", "http://schema-registry:8081");

        SchemaRegistryConfig config = SchemaRegistryConfig.fromProperties(props, "");

        assertEquals(SchemaRegistryBackend.CONFLUENT, config.backend());
        assertEquals("http://schema-registry:8081", config.url());
        assertTrue(config.autoRegisterSchemas());
    }

    @Test
    void requiresUrlForConfluent() {
        Properties props = new Properties();
        props.setProperty("schema.registry.type", "confluent");

        assertThrows(IllegalArgumentException.class, () -> SchemaRegistryConfig.fromProperties(props, ""));
    }

    @Test
    void glueProviderIsNotYetImplemented() {
        SchemaRegistryConfig config = new SchemaRegistryConfig(
                SchemaRegistryBackend.GLUE, null, true, "us-west-2", "lakehouse", "BACKWARD");
        SchemaRegistrySerdeProvider provider = SchemaRegistrySerdeProviders.create(config);

        assertThrows(UnsupportedOperationException.class, () -> provider.valueDeserializer(String.class));
    }
}
