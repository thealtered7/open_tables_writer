package com.thealtered7;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class Type2DimensionConfigLoaderTest {

    @Test
    void usesDefaultsForDatapipelinesProperties(@TempDir Path tempDir) throws IOException {
        Path props = tempDir.resolve("type2.properties");
        Files.writeString(
                props,
                """
                kafka.bootstrap.servers=kafka:9092
                kafka.client.id=create-type2-dimension
                kafka.group.id=create-type2-dimension
                """);

        Type2DimensionConfigLoader config = Type2DimensionConfigLoader.loadFromPath(props);

        assertEquals("", config.datapipelinesBaseUrl());
        assertEquals("lakehouse", config.datapipelinesCatalogName());
        assertFalse(config.datapipelinesJwtEnabled());
        assertEquals("open-table-write-notifications.dlq", config.dlqTopic());
    }

    @Test
    void readsDatapipelinesProperties(@TempDir Path tempDir) throws IOException {
        Path props = tempDir.resolve("type2.properties");
        Files.writeString(
                props,
                """
                kafka.bootstrap.servers=kafka:9092
                kafka.client.id=create-type2-dimension
                kafka.group.id=create-type2-dimension
                datapipelines.http.base-url=http://localhost:8080
                datapipelines.catalog.name=silver
                datapipelines.http.jwt.enabled=true
                """);

        Type2DimensionConfigLoader config = Type2DimensionConfigLoader.loadFromPath(props);

        assertEquals("http://localhost:8080", config.datapipelinesBaseUrl());
        assertEquals("silver", config.datapipelinesCatalogName());
        assertTrue(config.datapipelinesJwtEnabled());
    }
}
