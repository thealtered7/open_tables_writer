package com.thealtered7;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class CreateType2Dimension {

    private static final Logger log = LoggerFactory.getLogger(CreateType2Dimension.class);
    private static final String ENV_PROPERTIES_PATH = "TYPE2_DIMENSION_PROPERTIES_PATH";
    private static final Path DEFAULT_SILVER_WAREHOUSE = Path.of("/opt/data/silver");

    private CreateType2Dimension() {}

    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Usage: CreateType2Dimension <source-iceberg-table-path>");
            System.exit(1);
        }

        Path sourceTablePath = Path.of(args[0]);
        Path silverWarehouse = resolveSilverWarehouse();
        log.info("source table path: {}", sourceTablePath);
        log.info("silver warehouse path: {}", silverWarehouse);
        TableIdentity table = IcebergTableIdentity.fromTablePath(sourceTablePath);
        SparkSession spark = new SparkSessionFactory()
                .createIcebergTableSparkSession(table.getWarehouse(), silverWarehouse);
        Type2DimensionTransformer transformer = new Type2DimensionTransformer();
        try {
            transformer.transform(spark, table);
        } finally {
            spark.stop();
        }
    }

    private static Path resolveSilverWarehouse() {
        String propsPath = System.getenv(ENV_PROPERTIES_PATH);
        if (propsPath != null && !propsPath.isBlank()) {
            Properties props = loadProperties(Path.of(propsPath));
            return Path.of(props.getProperty("silver.warehouse.path", DEFAULT_SILVER_WAREHOUSE.toString()));
        }
        return Path.of(System.getProperty("silver.warehouse.path", DEFAULT_SILVER_WAREHOUSE.toString()));
    }

    private static Properties loadProperties(Path path) {
        if (!Files.isRegularFile(path)) {
            log.error("Properties file does not exist: {}", path);
            System.exit(1);
        }
        try (InputStream in = Files.newInputStream(path)) {
            Properties props = new Properties();
            props.load(in);
            return props;
        } catch (IOException e) {
            log.error("Failed to read properties from {}", path, e);
            System.exit(1);
            throw new AssertionError("unreachable");
        }
    }
}
