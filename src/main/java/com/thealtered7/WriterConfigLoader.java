package com.thealtered7;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class WriterConfigLoader {

    private static final Logger log = LoggerFactory.getLogger(WriterConfigLoader.class);
    private static final String ENV_COMMON_PROPERTIES_PATH = "WRITER_COMMON_PROPERTIES_PATH";
    private static final String ENV_WRITER_PROPERTIES_PATH = "WRITER_PROPERTIES_PATH";

    private final Properties properties;

    private WriterConfigLoader(Properties properties) {
        this.properties = properties;
    }

    public static WriterConfigLoader load() {
        String commonPath = System.getenv(ENV_COMMON_PROPERTIES_PATH);
        if (commonPath == null || commonPath.isBlank()) {
            log.error(
                    "Environment variable {} is not set. Set it to the path of writer-common.properties, "
                            + "e.g. config/writer-common.properties",
                    ENV_COMMON_PROPERTIES_PATH);
            System.exit(1);
        }
        String writerPath = System.getenv(ENV_WRITER_PROPERTIES_PATH);
        if (writerPath == null || writerPath.isBlank()) {
            log.error(
                    "Environment variable {} is not set. Set it to the path of the writer-specific properties "
                            + "file, e.g. config/delta-table-writer.properties",
                    ENV_WRITER_PROPERTIES_PATH);
            System.exit(1);
        }

        Properties merged = new Properties();
        loadFromPath(Path.of(commonPath), merged);
        loadFromPath(Path.of(writerPath), merged);
        log.info("Loaded writer properties from {} and {}", commonPath, writerPath);
        return new WriterConfigLoader(merged);
    }

    static WriterConfigLoader loadFromPaths(Path commonPath, Path writerPath) {
        Properties merged = new Properties();
        loadFromPath(commonPath, merged);
        loadFromPath(writerPath, merged);
        return new WriterConfigLoader(merged);
    }

    private static void loadFromPath(Path path, Properties target) {
        if (!Files.isRegularFile(path)) {
            log.error("Properties file does not exist: {}", path);
            System.exit(1);
        }
        try (InputStream in = Files.newInputStream(path)) {
            Properties loaded = new Properties();
            loaded.load(in);
            target.putAll(loaded);
        } catch (IOException e) {
            log.error("Failed to read properties from {}", path, e);
            System.exit(1);
        }
    }

    public String bootstrapServers() {
        return requireProperty("kafka.bootstrap.servers");
    }

    public String topic() {
        return requireProperty("kafka.topic");
    }

    public String clientId() {
        return requireProperty("kafka.client.id");
    }

    public String groupId() {
        return requireProperty("kafka.group.id");
    }

    public String dataDirectoryBasePath() {
        return requireProperty("data.directory.base.path");
    }

    public Duration inputFileWaitMax() {
        return Duration.ofSeconds(longProperty("input.file.wait.max.seconds", 10));
    }

    public Duration inputFileWaitPollInterval() {
        return Duration.ofMillis(longProperty("input.file.wait.poll.millis", 500));
    }

    private long longProperty(String key, long defaultValue) {
        String value = properties.getProperty(key);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            log.warn("Invalid value for {}: {}; using default {}", key, value, defaultValue);
            return defaultValue;
        }
    }

    private String requireProperty(String key) {
        String value = properties.getProperty(key);
        if (value == null || value.isBlank()) {
            log.error("Missing required property: {}", key);
            System.exit(1);
        }
        return value;
    }
}
