package com.thealtered7;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import com.thealtered7.datapipelines.KafkaWriteContext;
import com.thealtered7.datapipelines.RecordingDatapipelinesClient;
import com.thealtered7.datapipelines.TableWriteRegistration;
import com.thealtered7.models.FileFlushNotification;
import com.thealtered7.observability.Observability;
import com.thealtered7.schemaregistry.TableSchemaRegistrar;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

class IcebergTableWriterTest {

    private static final String SAMPLE_JSON_LINE = """
            {"extract":{"extract_job_id":"job-1","extract_buffer_id":"buf-1","extract_type":"cdc","extracted_at":"2026-05-31T02:50:00.000000Z"},"schema":{"type":"struct","fields":[]},"payload":{"before":{"id":1,"name":"scalar","value":1.06,"created_at":"2026-05-24T02:49:32.359424Z","updated_at":"2026-05-31T01:55:32.887469Z"},"after":{"id":1,"name":"scalar","value":1.04,"created_at":"2026-05-24T02:49:32.359424Z","updated_at":"2026-05-31T02:27:24.242870Z"},"source":{"version":"3.5.0.Final","connector":"postgresql","name":"extract","ts_ms":1780194444244,"snapshot":"false","db":"geo","sequence":"[\\"32589016\\",\\"32591512\\"]","ts_us":1780194444244620,"ts_ns":1780194444244620000,"schema":"public","table":"scalars","txId":22179,"lsn":32591512,"xmin":null,"origin":null,"origin_lsn":null},"transaction":null,"op":"u","ts_ms":1780194444583,"ts_us":1780194444583639,"ts_ns":1780194444583639448}}
            """;

    private static SparkSession spark;

    @BeforeAll
    static void startSpark() {
        spark = new SparkSessionFactory()
                .createIcebergTableSparkSession(Path.of("build/test-iceberg-warehouse"));
    }

    @AfterAll
    static void stopSpark() {
        if (spark != null) {
            spark.stop();
            SparkSession.clearActiveSession();
            SparkSession.clearDefaultSession();
        }
    }

    @Test
    void getTableFqnExtractsTableFromStandardFilename() {
        IcebergTableWriter writer = new IcebergTableWriter();
        Path inputFile = Path.of("geo.public.scalars-2026-05-31_02-51-21.jsonl");

        assertEquals("geo.public_bronze.scalars", invokeGetTableFqn(writer, inputFile));
    }

    @Test
    void getTableFqnStripsJsonlSuffixWhenPatternDoesNotMatch() {
        IcebergTableWriter writer = new IcebergTableWriter();
        Path inputFile = Path.of("custom.table.name.jsonl");

        assertEquals("custom.table_bronze.name", invokeGetTableFqn(writer, inputFile));
    }

    @Test
    void writeToTableCreatesThenAppendsIcebergTable(@TempDir Path tempDir) throws Exception {
        Path inputFile = tempDir.resolve("geo.public.scalars-2026-05-31_02-51-21.jsonl");
        Files.writeString(inputFile, SAMPLE_JSON_LINE);
        Path dataDirectory = tempDir.resolve("warehouse");

        IcebergTableWriter writer = new IcebergTableWriter();
        writer.writeToTable(spark, inputFile, dataDirectory);

        String catalogTable = "local_catalog.geo.public_bronze.scalars";

        Dataset<Row> created = spark.table(catalogTable);
        assertEquals(1L, created.count());
        Row createdRow = created.first();
        assertNotNull(createdRow.getAs("_extracted_at"));
        assertNotNull(createdRow.getAs("_transformed_at"));
        assertTrue(Set.of(created.columns()).contains("_extracted_at"));
        assertTrue(Set.of(created.columns()).contains("_transformed_at"));

        Files.writeString(inputFile, SAMPLE_JSON_LINE);
        writer.writeToTable(spark, inputFile, dataDirectory);

        Dataset<Row> appended = spark.table(catalogTable);
        assertEquals(2L, appended.count());
        assertTrue(spark.catalog().tableExists(catalogTable));
    }

    @Test
    void writeToTableLoadsFlattensAndPrintsRows(@TempDir Path tempDir) throws Exception {
        Path inputFile = tempDir.resolve("geo.public.flatten_check-2026-05-31_02-51-21.jsonl");
        Files.writeString(inputFile, SAMPLE_JSON_LINE.repeat(12));
        Path dataDirectory = tempDir.resolve("warehouse");

        DebeziumPayloadFlattener flattener = new DebeziumPayloadFlattener();
        Dataset<Row> raw = flattener.loadJsonLines(spark, inputFile);
        Dataset<Row> flat = flattener.flattenPayload(raw);
        Set<String> columns = Set.of(flat.columns());

        assertTrue(columns.contains("before_id"));
        assertTrue(columns.contains("after_id"));
        assertTrue(columns.contains("_source_db"));
        assertTrue(columns.contains("_op"));
        assertTrue(columns.contains("_extract_job_id"));

        IcebergTableWriter writer = new IcebergTableWriter();
        writer.writeToTable(spark, inputFile, dataDirectory);
    }

    @Test
    void writeToTableRegistersBronzeWriteWithKafkaMetadata(@TempDir Path tempDir) throws Exception {
        Path inputFile = tempDir.resolve("geo.public.register_check-2026-05-31_02-51-21.jsonl");
        Files.writeString(inputFile, SAMPLE_JSON_LINE);
        Path dataDirectory = tempDir.resolve("warehouse");

        RecordingDatapipelinesClient client = new RecordingDatapipelinesClient();
        KafkaWriteContext kafka = new KafkaWriteContext("cdc-file-write", 1, 99L);
        SourceTableIdentity source =
                new SourceTableIdentity("test-instance", "geo", "public", "register_check");
        Instant extractStart = Instant.parse("2026-05-31T02:50:21Z");
        Instant extractEnd = Instant.parse("2026-05-31T02:51:21Z");
        FileFlushNotification flush = sampleFlush(
                inputFile.toString(),
                "job-1",
                "buf-1",
                "cdc",
                extractStart,
                extractEnd,
                "test-instance",
                "geo",
                "public",
                "register_check");
        IcebergTableWriter writer = new IcebergTableWriter(Observability.noop(), client);
        writer.writeToTable(spark, inputFile, dataDirectory, kafka, source, flush);

        assertEquals(1, client.bronzeWrites().size());
        assertEquals(0, client.type2Writes().size());
        TableWriteRegistration registration = client.bronzeWrites().get(0);
        assertEquals(TableWriteRegistration.WRITE_TYPE_BRONZE, registration.writeType());
        assertEquals("test-instance", registration.sourceInstanceName());
        assertEquals("geo", registration.sourceDatabaseName());
        assertEquals("public", registration.sourceSchemaName());
        assertEquals("register_check", registration.sourceTableName());
        assertEquals("geo", registration.databaseName());
        assertEquals("public_bronze", registration.namespaceName());
        assertEquals("register_check", registration.tableName());
        assertEquals(1L, registration.writeRowCount());
        assertEquals(null, registration.mergeRowCount());
        assertEquals(inputFile.toString(), registration.rawFilePath());
        assertEquals(100L, registration.rawFileSize());
        assertEquals("job-1", registration.extractJobId());
        assertEquals("buf-1", registration.extractBufferId());
        assertEquals("cdc", registration.extractType());
        assertEquals(extractStart, registration.extractStartAt());
        assertEquals(extractEnd, registration.extractEndAt());
        assertEquals(null, registration.mergeStartAt());
        assertEquals(null, registration.mergeEndAt());
        assertEquals(dataDirectory.toAbsolutePath().toString(), registration.warehousePath());
        assertEquals(kafka, registration.kafka());
        assertNull(registration.keySchema());
        assertNull(registration.keySchemaId());
        assertNotNull(registration.valueSchema());
        assertTrue(registration.valueSchema().contains("\"type\""));
        assertNull(registration.valueSchemaId());
    }

    @Test
    void writeToTableRegistersSparkSchemaWithRegistrar(@TempDir Path tempDir) throws Exception {
        Path inputFile = tempDir.resolve("geo.public.schema_reg-2026-05-31_02-51-21.jsonl");
        Files.writeString(inputFile, SAMPLE_JSON_LINE);
        Path dataDirectory = tempDir.resolve("warehouse-schema-reg");

        RecordingDatapipelinesClient client = new RecordingDatapipelinesClient();
        List<String> subjects = new ArrayList<>();
        List<String> schemas = new ArrayList<>();
        TableSchemaRegistrar registrar = (subject, sparkSchemaJson) -> {
            subjects.add(subject);
            schemas.add(sparkSchemaJson);
            return "77";
        };
        SourceTableIdentity source =
                new SourceTableIdentity("test-instance", "geo", "public", "schema_reg");
        FileFlushNotification flush = sampleFlush(
                inputFile.toString(),
                "job-schema",
                "buf-schema",
                "cdc",
                Instant.parse("2026-05-31T02:50:21Z"),
                Instant.parse("2026-05-31T02:51:21Z"),
                "test-instance",
                "geo",
                "public",
                "schema_reg");
        IcebergTableWriter writer = new IcebergTableWriter(Observability.noop(), client, registrar);
        writer.writeToTable(spark, inputFile, dataDirectory, null, source, flush);

        assertEquals(List.of("iceberg.geo.public_bronze.schema_reg-value"), subjects);
        assertEquals(1, schemas.size());
        assertTrue(schemas.get(0).contains("\"type\""));
        TableWriteRegistration registration = client.bronzeWrites().get(0);
        assertEquals(schemas.get(0), registration.valueSchema());
        assertEquals("77", registration.valueSchemaId());
        assertNull(registration.keySchema());
        assertNull(registration.keySchemaId());
    }

    @Test
    void writeToTableSkipsRegistrationWhenExtractTimesMissing(@TempDir Path tempDir) throws Exception {
        Path inputFile = tempDir.resolve("geo.public.skip_extract-2026-05-31_02-51-21.jsonl");
        Files.writeString(inputFile, SAMPLE_JSON_LINE);
        Path dataDirectory = tempDir.resolve("warehouse-skip");

        RecordingDatapipelinesClient client = new RecordingDatapipelinesClient();
        SourceTableIdentity source =
                new SourceTableIdentity("test-instance", "geo", "public", "skip_extract");
        IcebergTableWriter writer = new IcebergTableWriter(Observability.noop(), client);
        writer.writeToTable(spark, inputFile, dataDirectory, null, source, null);

        assertEquals(0, client.bronzeWrites().size());
        assertTrue(spark.catalog().tableExists("local_catalog.geo.public_bronze.skip_extract"));
    }

    @Test
    void writeToTableContinuesWhenDatapipelinesRegistrationFails(@TempDir Path tempDir) throws Exception {
        Path inputFile = tempDir.resolve("geo.public.register_fail-2026-05-31_02-51-21.jsonl");
        Files.writeString(inputFile, SAMPLE_JSON_LINE);
        Path dataDirectory = tempDir.resolve("warehouse-fail");

        RecordingDatapipelinesClient client = new RecordingDatapipelinesClient();
        client.failPosts();
        SourceTableIdentity source =
                new SourceTableIdentity("test-instance", "geo", "public", "register_fail");
        FileFlushNotification flush = sampleFlush(
                inputFile.toString(),
                "job-fail",
                "buf-fail",
                "cdc",
                Instant.parse("2026-05-31T02:50:21Z"),
                Instant.parse("2026-05-31T02:51:21Z"),
                "test-instance",
                "geo",
                "public",
                "register_fail");
        IcebergTableWriter writer = new IcebergTableWriter(Observability.noop(), client);

        assertDoesNotThrow(() -> writer.writeToTable(spark, inputFile, dataDirectory, null, source, flush));
        assertTrue(spark.catalog().tableExists("local_catalog.geo.public_bronze.register_fail"));
        assertEquals(0, client.bronzeWrites().size());
    }

    @Test
    void oneShotRequiresInputFilePathSystemProperty() {
        System.clearProperty("input.file.path");
        System.clearProperty("data.directory.base.path");

        assertThrowsExactly(NullPointerException.class, () -> IcebergTableWriterOneShot.main(new String[] {}));
    }

    private static FileFlushNotification sampleFlush(
            String rawFilePath,
            String extractJobId,
            String extractBufferId,
            String extractType,
            Instant extractStartAt,
            Instant extractEndAt,
            String sourceInstanceName,
            String sourceDatabaseName,
            String sourceSchemaName,
            String sourceTableName) {
        return new FileFlushNotification(
                "raw",
                rawFilePath,
                sourceDatabaseName + "." + sourceSchemaName + "." + sourceTableName,
                extractJobId,
                extractBufferId,
                extractType,
                extractStartAt,
                extractEndAt,
                1L,
                100L,
                sourceInstanceName,
                sourceDatabaseName,
                sourceSchemaName,
                sourceTableName,
                "/opt/data/raw/",
                null,
                null,
                null,
                null,
                null,
                null);
    }

    private static String invokeGetTableFqn(IcebergTableWriter writer, Path inputFile) {
        try {
            var method = IcebergTableWriter.class.getDeclaredMethod("getTableFqn", Path.class);
            method.setAccessible(true);
            return (String) method.invoke(writer, inputFile);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

}
