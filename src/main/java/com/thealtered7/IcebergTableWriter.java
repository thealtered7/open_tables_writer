package com.thealtered7;

import com.thealtered7.datapipelines.DatapipelinesClient;
import com.thealtered7.datapipelines.KafkaWriteContext;
import com.thealtered7.datapipelines.NoopDatapipelinesClient;
import com.thealtered7.datapipelines.TableWriteRegistration;
import com.thealtered7.models.FileFlushNotification;
import com.thealtered7.observability.Observability;
import com.thealtered7.schema.IcebergSchemaEvolver;
import com.thealtered7.schemaregistry.TableSchemaRegistrar;
import com.thealtered7.schemaregistry.TableSchemaRegistrars;
import com.thealtered7.schemaregistry.TableSchemaSubjects;
import com.thealtered7.schemaregistry.SchemaRegistryConfig;
import java.nio.file.Path;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.spark.sql.functions.current_timestamp;

public class IcebergTableWriter implements TableWriter {
    private static final Logger log = LoggerFactory.getLogger(IcebergTableWriter.class);
    private static final String INCOMING_VIEW = "incoming_cdc";
    private static final String WAREHOUSE_CONFIG = "spark.sql.catalog.local_catalog.warehouse";
    private static final String TRANSFORMED_AT_COLUMN = "_transformed_at";

    private final Observability observability;
    private final DatapipelinesClient datapipelinesClient;
    private final TableSchemaRegistrar tableSchemaRegistrar;

    public IcebergTableWriter() {
        this(Observability.noop());
    }

    public IcebergTableWriter(Observability observability) {
        this(observability, new NoopDatapipelinesClient());
    }

    public IcebergTableWriter(Observability observability, DatapipelinesClient datapipelinesClient) {
        this(observability, datapipelinesClient, TableSchemaRegistrars.create(SchemaRegistryConfig.none()));
    }

    public IcebergTableWriter(
            Observability observability,
            DatapipelinesClient datapipelinesClient,
            TableSchemaRegistrar tableSchemaRegistrar) {
        this.observability = Objects.requireNonNull(observability, "observability");
        this.datapipelinesClient = Objects.requireNonNull(datapipelinesClient, "datapipelinesClient");
        this.tableSchemaRegistrar =
                Objects.requireNonNull(tableSchemaRegistrar, "tableSchemaRegistrar");
    }

    private String getTableFqn(Path inputFilePath) {
        return OpenTableNamespaces.toBronzeTableFqn(
                CdcInputFileNames.tableFqnFromFileName(inputFilePath.getFileName().toString()));
    }

    @Override
    public void writeToTable(
            SparkSession spark,
            Path inputFilePath,
            Path dataDirectoryBasePath,
            KafkaWriteContext kafka,
            SourceTableIdentity source,
            FileFlushNotification flush) {
        String tableFQN = this.getTableFqn(inputFilePath);
        try {
            Map<String, String> tags = new HashMap<>();
            tags.put("table", tableFQN);
            tags.put("input_file", inputFilePath.toString());
            if (flush != null) {
                tags.put(ExtractMdc.EXTRACT_JOB_ID, ExtractMdc.normalize(flush.extractJobId()));
                tags.put(ExtractMdc.EXTRACT_BUFFER_ID, ExtractMdc.normalize(flush.extractBufferId()));
            }
            observability.observeCallableVoid(
                    Observability.ICEBERG_TABLE_WRITER_PREFIX,
                    "write_to_table",
                    tags,
                    () -> {
                        writeToTableInternal(
                                spark, inputFilePath, dataDirectoryBasePath, tableFQN, kafka, source, flush);
                        return "success";
                    });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void writeToTableInternal(
            SparkSession spark,
            Path inputFilePath,
            Path dataDirectoryBasePath,
            String tableFQN,
            KafkaWriteContext kafka,
            SourceTableIdentity source,
            FileFlushNotification flush) {
        log.info("writing to table: {}", inputFilePath.getFileName());

        spark.conf().set(WAREHOUSE_CONFIG, dataDirectoryBasePath.toAbsolutePath().toString());

        log.info("table FQN: {}", tableFQN);
        DebeziumPayloadFlattener flattener = new DebeziumPayloadFlattener(observability);
        Dataset<Row> raw = flattener.loadRawFile(spark, inputFilePath);
        Dataset<Row> flat = flattener.flattenPayload(raw);
        Dataset<Row> withTimestamps = flattener.convertTimestampColumns(flat);
        withTimestamps.show(10, false);

        Date now = new Date();
        Dataset<Row> partitioned = flattener.addDatePartitionColumns(withTimestamps, now);
        Path outputTablePath = flattener.getOutputTablePath(tableFQN, dataDirectoryBasePath);
        String catalogTable = toCatalogTableName(tableFQN);
        String sqlTable = toSqlTableName(tableFQN);
        log.info("output table path: {}", outputTablePath);
        log.info("catalog table: {}", catalogTable);

        String valueSchema = flush == null ? null : flush.valueSchema();
        Dataset<Row> withTransformedAt =
                partitioned.withColumn(TRANSFORMED_AT_COLUMN, current_timestamp());
        Dataset<Row> aligned = IcebergSchemaEvolver.evolveAndAlign(
                spark,
                catalogTable,
                sqlTable,
                withTransformedAt,
                valueSchema,
                IcebergSchemaEvolver.LayerMode.BRONZE);

        aligned.createOrReplaceTempView(INCOMING_VIEW);
        long rowCount = aligned.count();

        if (!spark.catalog().tableExists(catalogTable)) {
            log.info("creating iceberg table {}", catalogTable);
            spark.sql(String.format(
                    """
                    CREATE TABLE %s
                    USING iceberg
                    PARTITIONED BY (_year, _month, _day)
                    AS SELECT * FROM %s
                    """,
                    sqlTable,
                    INCOMING_VIEW));
        } else {
            log.info("appending to iceberg table {}", catalogTable);
            spark.sql(String.format(
                    """
                    INSERT INTO %s
                    SELECT * FROM %s
                    """,
                    sqlTable,
                    INCOMING_VIEW));
        }

        spark.catalog().refreshTable(catalogTable);
        String tableSchemaJson = spark.table(catalogTable).schema().json();
        String tableSchemaId = null;
        if (source != null && source.isComplete()) {
            String schemaSubject = TableSchemaSubjects.valueSubject(
                    source.databaseName(),
                    OpenTableNamespaces.bronze(source.schemaName()),
                    source.tableName());
            tableSchemaId = tableSchemaRegistrar.register(schemaSubject, tableSchemaJson);
        }

        registerBronzeWrite(
                tableFQN,
                rowCount,
                dataDirectoryBasePath,
                kafka,
                source,
                flush,
                tableSchemaJson,
                tableSchemaId);
    }

    private void registerBronzeWrite(
            String tableFQN,
            long rowCount,
            Path dataDirectoryBasePath,
            KafkaWriteContext kafka,
            SourceTableIdentity source,
            FileFlushNotification flush,
            String tableSchemaJson,
            String tableSchemaId) {
        if (source == null || !source.isComplete()) {
            log.warn("Skipping datapipelines registration; missing source identity for {}", tableFQN);
            return;
        }
        if (flush == null || flush.extractStartAt() == null || flush.extractEndAt() == null) {
            log.warn(
                    "Skipping datapipelines registration; missing extract_start_at/extract_end_at for {}",
                    tableFQN);
            return;
        }
        try {
            datapipelinesClient.postTableWrite(new TableWriteRegistration(
                    TableWriteRegistration.WRITE_TYPE_BRONZE,
                    source.databaseName(),
                    OpenTableNamespaces.bronze(source.schemaName()),
                    source.tableName(),
                    source.instanceName(),
                    source.databaseName(),
                    source.schemaName(),
                    source.tableName(),
                    rowCount,
                    null,
                    flush.rawFilePath(),
                    flush.rawFileSize(),
                    flush.extractJobId(),
                    flush.extractBufferId(),
                    flush.extractType(),
                    flush.extractStartAt(),
                    flush.extractEndAt(),
                    null,
                    null,
                    dataDirectoryBasePath.toAbsolutePath().toString(),
                    kafka,
                    null,
                    tableSchemaJson,
                    null,
                    tableSchemaId,
                    flush.sourceMinLsn(),
                    flush.sourceMaxLsn()));
        } catch (RuntimeException e) {
            log.error("Failed to register bronze iceberg table write for {}", tableFQN, e);
        }
    }

    @Override
    public com.thealtered7.models.TableUpdatedNotification.OpenTableFormat format() {
        return com.thealtered7.models.TableUpdatedNotification.OpenTableFormat.ICEBERG;
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
        new TableWriterKafkaDaemon(
                        (obs, client, registrar) -> new IcebergTableWriter(obs, client, registrar),
                        base -> new SparkSessionFactory().createIcebergTableSparkSession(base))
                .run();
    }
}
