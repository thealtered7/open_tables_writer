package com.thealtered7;

import java.nio.file.Path;
import java.util.Date;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergTableWriter {
    private static final Logger log = LoggerFactory.getLogger(IcebergTableWriter.class);
    private static final Pattern INPUT_FILE_NAME =
            Pattern.compile("^(.+)-(\\d{4}-\\d{2}-\\d{2}_\\d{2}-\\d{2}-\\d{2})\\.jsonl$");
    private static final String MERGE_KEY = "after_id";
    private static final String INCOMING_VIEW = "incoming_cdc";
    private static final String WAREHOUSE_CONFIG = "spark.sql.catalog.local_catalog.warehouse";

    public IcebergTableWriter() {
    }

    private String getTableFqn(Path inputFilePath) {
        // geo.public.scalars-2026-05-31_02-51-21.jsonl
        String fileName = inputFilePath.getFileName().toString();
        Matcher matcher = INPUT_FILE_NAME.matcher(fileName);
        if (matcher.matches()) {
            return matcher.group(1);
        }
        return fileName.replaceFirst("\\.jsonl$", "");
    }

    public void writeToTable(SparkSession spark, Path inputFilePath, Path dataDirectoryBasePath) {
        log.info("writing to table: {}", inputFilePath.getFileName());

        spark.conf().set(WAREHOUSE_CONFIG, dataDirectoryBasePath.toAbsolutePath().toString());

        String tableFQN = this.getTableFqn(inputFilePath);
        log.info("table FQN: {}", tableFQN);
        DebeziumPayloadFlattener flattener = new DebeziumPayloadFlattener();
        Dataset<Row> raw = flattener.loadJsonLines(spark, inputFilePath);
        Dataset<Row> flat = flattener.flattenPayload(raw);
        Dataset<Row> withTimestamps = flattener.convertTimestampColumns(flat);
        withTimestamps.show(10, false);

        Date now = new Date();
        Dataset<Row> partitioned = flattener.addDatePartitionColumns(withTimestamps, now)
                .dropDuplicates(MERGE_KEY, "year", "month", "day");
        Path outputTablePath = flattener.getOutputTablePath(tableFQN, dataDirectoryBasePath);
        String catalogTable = toCatalogTableName(tableFQN);
        log.info("output table path: {}", outputTablePath);
        log.info("catalog table: {}", catalogTable);

        partitioned.createOrReplaceTempView(INCOMING_VIEW);

        if (!spark.catalog().tableExists(catalogTable)) {
            log.info("creating iceberg table {}", catalogTable);
            spark.sql(String.format(
                    """
                    CREATE TABLE %s
                    USING iceberg
                    PARTITIONED BY (year, month, day)
                    AS SELECT * FROM %s
                    """,
                    toSqlTableName(tableFQN),
                    INCOMING_VIEW));
        } else {
            log.info("merging into iceberg table {}", catalogTable);
            spark.sql(String.format(
                    """
                    MERGE INTO %s AS t
                    USING %s AS s
                    ON t.%s = s.%s AND t.year = s.year AND t.month = s.month AND t.day = s.day
                    WHEN MATCHED THEN UPDATE SET *
                    WHEN NOT MATCHED THEN INSERT *
                    """,
                    toSqlTableName(tableFQN),
                    INCOMING_VIEW,
                    MERGE_KEY,
                    MERGE_KEY));
        }
    }

    private String toCatalogTableName(String tableFQN) {
        String[] parts = tableFQN.split("\\.");
        return String.format("local_catalog.%s.%s.%s", parts[0], parts[1], parts[2]);
    }

    private String toSqlTableName(String tableFQN) {
        String[] parts = tableFQN.split("\\.");
        return String.format("local_catalog.`%s`.`%s`.`%s`", parts[0], parts[1], parts[2]);
    }

    public static void main(String[] args) {
        String inputFile = System.getProperty("input.file.path");
        String dataDirectoryBase = System.getProperty("data.directory.base.path");
        Objects.requireNonNull(inputFile, "input.file.path is required");
        Objects.requireNonNull(dataDirectoryBase, "data.directory.base.path is required");
        Path inputFilePath = Path.of(inputFile);
        Path dataDirectoryBasePath = Path.of(dataDirectoryBase);
        dataDirectoryBasePath.toFile().mkdirs();

        SparkSession spark = new SparkSessionFactory().createIcebergTableSparkSession(dataDirectoryBasePath);
        try {
            new IcebergTableWriter().writeToTable(spark, inputFilePath, dataDirectoryBasePath);
        } finally {
            spark.stop();
        }
    }
}
