package com.thealtered7;

import com.thealtered7.schemaregistry.SchemaRegistryBackend;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WriterConfigLoaderTest {

    @Test
    void loadFromPathsMergesCommonAndWriterSpecificProperties(@TempDir Path tempDir) throws IOException {
        Path common = tempDir.resolve("common.properties");
        Path writer = tempDir.resolve("writer.properties");
        Files.writeString(
                common,
                """
                kafka.bootstrap.servers=kafka:9092
                kafka.topic=cdc-file-write
                """);
        Files.writeString(
                writer,
                """
                kafka.client.id=delta-table-writer
                kafka.group.id=delta-table-writer
                data.directory.base.path=/opt/data/deltatable
                """);

        WriterConfigLoader config = WriterConfigLoader.loadFromPaths(common, writer);

        assertEquals("kafka:9092", config.bootstrapServers());
        assertEquals("cdc-file-write", config.topic());
        assertEquals("cdc-file-write.dlq", config.dlqTopic());
        assertEquals("delta-table-writer", config.clientId());
        assertEquals("delta-table-writer", config.groupId());
        assertEquals("/opt/data/deltatable", config.dataDirectoryBasePath());
    }

    @Test
    void writerSpecificPropertiesOverrideCommon(@TempDir Path tempDir) throws IOException {
        Path common = tempDir.resolve("common.properties");
        Path writer = tempDir.resolve("writer.properties");
        Files.writeString(common, "kafka.topic=other-topic\n");
        Files.writeString(writer, "kafka.topic=cdc-file-write\n");

        WriterConfigLoader config = WriterConfigLoader.loadFromPaths(common, writer);

        assertEquals("cdc-file-write", config.topic());
    }

    @Test
    void usesDefaultsForOptionalInputFileWaitProperties(@TempDir Path tempDir) throws IOException {
        Path common = tempDir.resolve("common.properties");
        Path writer = tempDir.resolve("writer.properties");
        Files.writeString(common, "kafka.bootstrap.servers=kafka:9092\nkafka.topic=cdc-file-write\n");
        Files.writeString(
                writer,
                """
                kafka.client.id=delta-table-writer
                kafka.group.id=delta-table-writer
                data.directory.base.path=/opt/data/deltatable
                """);

        WriterConfigLoader config = WriterConfigLoader.loadFromPaths(common, writer);

        assertEquals(Duration.ofSeconds(10), config.inputFileWaitMax());
        assertEquals(Duration.ofMillis(500), config.inputFileWaitPollInterval());
    }

    @Test
    void readsOptionalInputFileWaitProperties(@TempDir Path tempDir) throws IOException {
        Path common = tempDir.resolve("common.properties");
        Path writer = tempDir.resolve("writer.properties");
        Files.writeString(
                common,
                """
                kafka.bootstrap.servers=kafka:9092
                kafka.topic=cdc-file-write
                input.file.wait.max.seconds=3
                input.file.wait.poll.millis=100
                """);
        Files.writeString(
                writer,
                """
                kafka.client.id=delta-table-writer
                kafka.group.id=delta-table-writer
                data.directory.base.path=/opt/data/deltatable
                """);

        WriterConfigLoader config = WriterConfigLoader.loadFromPaths(common, writer);

        assertEquals(Duration.ofSeconds(3), config.inputFileWaitMax());
        assertEquals(Duration.ofMillis(100), config.inputFileWaitPollInterval());
    }

    @Test
    void usesDefaultsForDatapipelinesProperties(@TempDir Path tempDir) throws IOException {
        Path common = tempDir.resolve("common.properties");
        Path writer = tempDir.resolve("writer.properties");
        Files.writeString(common, "kafka.bootstrap.servers=kafka:9092\nkafka.topic=cdc-file-write\n");
        Files.writeString(
                writer,
                """
                kafka.client.id=iceberg-table-writer
                kafka.group.id=iceberg-table-writer
                data.directory.base.path=/opt/data/icebergtable
                """);

        WriterConfigLoader config = WriterConfigLoader.loadFromPaths(common, writer);

        assertEquals("", config.datapipelinesBaseUrl());
        assertEquals("lakehouse", config.datapipelinesCatalogName());
        assertFalse(config.datapipelinesJwtEnabled());
    }

    @Test
    void readsSchemaRegistryProperties(@TempDir Path tempDir) throws IOException {
        Path common = tempDir.resolve("common.properties");
        Path writer = tempDir.resolve("writer.properties");
        Files.writeString(
                common,
                """
                kafka.bootstrap.servers=kafka:9092
                kafka.topic=cdc-file-write
                schema.registry.type=confluent
                schema.registry.url=http://schema-registry:8081
                """);
        Files.writeString(
                writer,
                """
                kafka.client.id=iceberg-table-writer
                kafka.group.id=iceberg-table-writer
                data.directory.base.path=/opt/data/icebergtable
                """);

        WriterConfigLoader config = WriterConfigLoader.loadFromPaths(common, writer);

        assertEquals(SchemaRegistryBackend.CONFLUENT, config.schemaRegistryConfig().backend());
        assertEquals("http://schema-registry:8081", config.schemaRegistryConfig().url());
    }

    @Test
    void readsDatapipelinesProperties(@TempDir Path tempDir) throws IOException {
        Path common = tempDir.resolve("common.properties");
        Path writer = tempDir.resolve("writer.properties");
        Files.writeString(
                common,
                """
                kafka.bootstrap.servers=kafka:9092
                kafka.topic=cdc-file-write
                datapipelines.http.base-url=http://datapipelines-app:8080
                datapipelines.catalog.name=bronze
                datapipelines.http.jwt.enabled=true
                """);
        Files.writeString(
                writer,
                """
                kafka.client.id=iceberg-table-writer
                kafka.group.id=iceberg-table-writer
                data.directory.base.path=/opt/data/icebergtable
                """);

        WriterConfigLoader config = WriterConfigLoader.loadFromPaths(common, writer);

        assertEquals("http://datapipelines-app:8080", config.datapipelinesBaseUrl());
        assertEquals("bronze", config.datapipelinesCatalogName());
        assertTrue(config.datapipelinesJwtEnabled());
    }
}
