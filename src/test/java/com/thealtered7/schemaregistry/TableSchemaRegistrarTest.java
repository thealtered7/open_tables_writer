package com.thealtered7.schemaregistry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class TableSchemaRegistrarTest {

    @Test
    void noneRegistrarAlwaysReturnsNull() {
        TableSchemaRegistrar registrar = TableSchemaRegistrars.create(SchemaRegistryConfig.none());
        assertNull(registrar.register("iceberg.geo.public_bronze.scalars-value", "{\"type\":\"struct\"}"));
    }

    @Test
    void glueRegistrarReturnsNull() {
        SchemaRegistryConfig config =
                new SchemaRegistryConfig(SchemaRegistryBackend.GLUE, null, true, "us-east-1", "lakehouse", null);
        TableSchemaRegistrar registrar = TableSchemaRegistrars.create(config);
        assertNull(registrar.register("iceberg.geo.public_bronze.scalars-value", "{\"type\":\"struct\"}"));
    }

    @Test
    void confluentRegistrarStringifiesRegistryId() {
        TableSchemaRegistrar registrar = new ConfluentTableSchemaRegistrar((subject, wrapped) -> 42);
        assertEquals(
                "42",
                registrar.register(
                        "iceberg.geo.public_bronze.scalars-value",
                        "{\"type\":\"struct\",\"fields\":[]}"));
    }

    @Test
    void confluentRegistrarReturnsNullOnFailure() {
        TableSchemaRegistrar registrar = new ConfluentTableSchemaRegistrar((subject, wrapped) -> {
            throw new RuntimeException("sr down");
        });
        assertNull(registrar.register("iceberg.geo.public_bronze.scalars-value", "{\"type\":\"struct\"}"));
    }

    @Test
    void wrapSparkSchemaEmbedsVendorExtension() throws Exception {
        String wrapped = ConfluentTableSchemaRegistrar.wrapSparkSchema("{\"type\":\"struct\",\"fields\":[]}");
        assertTrue(wrapped.contains("\"x-spark-schema\""));
        assertTrue(wrapped.contains("\"type\":\"struct\""));
        assertTrue(wrapped.contains("\"title\":\"spark-table-schema\""));
    }

    @Test
    void valueSubjectUsesCatalogStyleName() {
        assertEquals(
                "iceberg.geo.public_bronze.scalars-value",
                TableSchemaSubjects.valueSubject("geo", "public_bronze", "scalars"));
        assertEquals(
                "iceberg.geo.public_silver.scalars_type2-value",
                TableSchemaSubjects.valueSubject("geo", "public_silver", "scalars_type2"));
        assertEquals(
                "iceberg.geo.public_silver.scalars_type1-value",
                TableSchemaSubjects.valueSubject("geo", "public_silver", "scalars_type1"));
    }

    @Test
    void recordingRegistrarCapturesSubjectAndSchema() {
        List<String> subjects = new ArrayList<>();
        List<String> schemas = new ArrayList<>();
        TableSchemaRegistrar registrar = (subject, sparkSchemaJson) -> {
            subjects.add(subject);
            schemas.add(sparkSchemaJson);
            return "99";
        };
        assertEquals("99", registrar.register("iceberg.geo.public_bronze.t-value", "{\"fields\":[]}"));
        assertEquals(List.of("iceberg.geo.public_bronze.t-value"), subjects);
        assertEquals(List.of("{\"fields\":[]}"), schemas);
    }
}
