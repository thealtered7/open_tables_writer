package com.thealtered7;

import com.thealtered7.schemaregistry.SchemaRegistryConfig;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Type2DimensionConfigLoader {

    private static final Logger log = LoggerFactory.getLogger(Type2DimensionConfigLoader.class);
    private static final String ENV_PROPERTIES_PATH = "TYPE2_DIMENSION_PROPERTIES_PATH";
    private static final String DEFAULT_SILVER_WAREHOUSE = "/opt/data/silver";
    private static final String DEFAULT_NOTIFICATIONS_TOPIC = "open-table-write-notifications";

    private final Properties properties;

    private Type2DimensionConfigLoader(Properties properties) {
        this.properties = properties;
    }

    public static Type2DimensionConfigLoader load() {
        String path = System.getenv(ENV_PROPERTIES_PATH);
        if (path == null || path.isBlank()) {
            log.error(
                    "Environment variable {} is not set. Set it to the path of the type-2 dimension properties "
                            + "file, e.g. config/create-type2-dimension.properties",
                    ENV_PROPERTIES_PATH);
            System.exit(1);
        }
        return loadFromPath(Path.of(path));
    }

    static Type2DimensionConfigLoader loadFromPath(Path path) {
        if (!Files.isRegularFile(path)) {
            log.error("Properties file does not exist: {}", path);
            System.exit(1);
        }
        try (InputStream in = Files.newInputStream(path)) {
            Properties props = new Properties();
            props.load(in);
            log.info("Loaded type-2 dimension properties from {}", path);
            return new Type2DimensionConfigLoader(props);
        } catch (IOException e) {
            log.error("Failed to read properties from {}", path, e);
            System.exit(1);
            throw new AssertionError("unreachable");
        }
    }

    public String bootstrapServers() {
        return requireProperty("kafka.bootstrap.servers");
    }

    public String topic() {
        return properties.getProperty("kafka.opentable.write.notifications", DEFAULT_NOTIFICATIONS_TOPIC);
    }

    public String dlqTopic() {
        return properties.getProperty("kafka.dlq.topic", topic() + ".dlq");
    }

    public String clientId() {
        return requireProperty("kafka.client.id");
    }

    public String groupId() {
        return requireProperty("kafka.group.id");
    }

    public Path silverWarehousePath() {
        return Path.of(properties.getProperty("silver.warehouse.path", DEFAULT_SILVER_WAREHOUSE));
    }

    public String datapipelinesBaseUrl() {
        return properties.getProperty("datapipelines.http.base-url", "");
    }

    public String datapipelinesCatalogName() {
        return properties.getProperty("datapipelines.catalog.name", "lakehouse");
    }

    public boolean datapipelinesJwtEnabled() {
        return Boolean.parseBoolean(properties.getProperty("datapipelines.http.jwt.enabled", "false"));
    }

    public SchemaRegistryConfig schemaRegistryConfig() {
        return SchemaRegistryConfig.fromProperties(properties, "");
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
