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

public class DeltaTableWriter implements TableWriter {
    private static final Logger log = LoggerFactory.getLogger(DeltaTableWriter.class);
    private static final Pattern INPUT_FILE_NAME =
            Pattern.compile("^(.+)-(\\d{4}-\\d{2}-\\d{2}_\\d{2}-\\d{2}-\\d{2})\\.jsonl$");
    private static final String INCOMING_VIEW = "incoming_cdc";

    private final DeltaTableOperations deltaOperations;

    public DeltaTableWriter() {
        this(new DeltaTableOperations());
    }

    public DeltaTableWriter(DeltaTableOperations deltaOperations) {
        this.deltaOperations = deltaOperations;
    }

    private String getTableFqn(Path inputFilePath) {
        String fileName = inputFilePath.getFileName().toString();
        Matcher matcher = INPUT_FILE_NAME.matcher(fileName);
        if (matcher.matches()) {
            return matcher.group(1);
        }
        return fileName.replaceFirst("\\.jsonl$", "");
    }

    @Override
    public void writeToTable(SparkSession spark, Path inputFilePath, Path dataDirectoryBasePath) {
        log.info("writing to table: {}", inputFilePath.getFileName());

        String tableFQN = this.getTableFqn(inputFilePath);
        log.info("table FQN: {}", tableFQN);
        DebeziumPayloadFlattener flattener = new DebeziumPayloadFlattener();
        Dataset<Row> raw = flattener.loadJsonLines(spark, inputFilePath);
        Dataset<Row> flat = flattener.flattenPayload(raw);
        Dataset<Row> withTimestamps = flattener.convertTimestampColumns(flat);
        withTimestamps.show(10, false);

        Date now = new Date();
        Dataset<Row> partitioned = flattener.addDatePartitionColumns(withTimestamps, now);
        Path outputTablePath = flattener.getOutputTablePath(tableFQN, dataDirectoryBasePath);
        log.info("output table path: {}", outputTablePath);

        partitioned.createOrReplaceTempView(INCOMING_VIEW);

        if (!deltaOperations.tableExists(spark, outputTablePath)) {
            log.info("creating delta table at {}", outputTablePath);
            deltaOperations.createPartitionedTable(partitioned, outputTablePath);
        } else {
            log.info("appending to delta table at {}", outputTablePath);
            deltaOperations.appendToTable(partitioned, outputTablePath);
        }
    }

    public void run() {
        String inputFile = System.getProperty("input.file.path");
        String dataDirectoryBase = System.getProperty("data.directory.base.path");
        Objects.requireNonNull(inputFile, "input.file.path is required");
        Objects.requireNonNull(dataDirectoryBase, "data.directory.base.path is required");
        Path inputFilePath = Path.of(inputFile);
        Path dataDirectoryBasePath = Path.of(dataDirectoryBase);
        dataDirectoryBasePath.toFile().mkdirs();

        SparkSession spark = new SparkSessionFactory().createDeltaTableSparkSession(dataDirectoryBasePath);
        try {
            writeToTable(spark, inputFilePath, dataDirectoryBasePath);
        } finally {
            spark.stop();
        }
    }

    public static void main(String[] args) {
        new DeltaTableWriter().run();
    }
}
