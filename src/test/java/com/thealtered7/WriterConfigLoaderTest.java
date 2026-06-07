package com.thealtered7;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
}
