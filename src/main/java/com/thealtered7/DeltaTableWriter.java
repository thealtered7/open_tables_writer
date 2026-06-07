package com.thealtered7;

import com.thealtered7.observability.Observability;
import java.nio.file.Path;
import java.util.Date;
import java.util.Map;
import java.util.Objects;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeltaTableWriter implements TableWriter {
    private static final Logger log = LoggerFactory.getLogger(DeltaTableWriter.class);
    private static final String INCOMING_VIEW = "incoming_cdc";

    private final DeltaTableOperations deltaOperations;
    private final Observability observability;

    public DeltaTableWriter() {
        this(new DeltaTableOperations());
    }

    public DeltaTableWriter(DeltaTableOperations deltaOperations) {
        this(deltaOperations, Observability.noop());
    }

    public DeltaTableWriter(Observability observability) {
        this(new DeltaTableOperations(), observability);
    }

    public DeltaTableWriter(DeltaTableOperations deltaOperations, Observability observability) {
        this.deltaOperations = deltaOperations;
        this.observability = Objects.requireNonNull(observability, "observability");
    }

    private String getTableFqn(Path inputFilePath) {
        return CdcInputFileNames.tableFqnFromFileName(inputFilePath.getFileName().toString());
    }

    @Override
    public void writeToTable(SparkSession spark, Path inputFilePath, Path dataDirectoryBasePath) {
        String tableFQN = this.getTableFqn(inputFilePath);
        try {
            observability.observeCallableVoid(
                    Observability.DELTA_TABLE_WRITER_PREFIX,
                    "write_to_table",
                    Map.of("table", tableFQN, "input_file", inputFilePath.toString()),
                    () -> {
                        writeToTableInternal(spark, inputFilePath, dataDirectoryBasePath, tableFQN);
                        return "success";
                    });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void writeToTableInternal(
            SparkSession spark, Path inputFilePath, Path dataDirectoryBasePath, String tableFQN) {
        log.info("writing to table: {}", inputFilePath.getFileName());
        log.info("table FQN: {}", tableFQN);
        DebeziumPayloadFlattener flattener = new DebeziumPayloadFlattener(observability);
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

    public static void main(String[] args) {
        new TableWriterKafkaDaemon(
                        obs -> new DeltaTableWriter(obs),
                        base -> new SparkSessionFactory().createDeltaTableSparkSession(base))
                .run();
    }
}
