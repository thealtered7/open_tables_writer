package com.thealtered7;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import com.thealtered7.datapipelines.KafkaWriteContext;
import com.thealtered7.datapipelines.RecordingDatapipelinesClient;
import com.thealtered7.datapipelines.TableWriteRegistration;
import com.thealtered7.observability.Observability;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class Type2DimensionTransformerTest {

    private static final String END_OF_TIME_LITERAL = "10000-12-31 23:59:59.999999";

    private static final String BUFFER_1 = "buf-1";
    private static final String BUFFER_2 = "buf-2";
    private static final String BUFFER_3 = "buf-3";
    private static final String BUFFER_4 = "buf-4";
    private static final String BUFFER_5 = "buf-5";
    private static final String BUFFER_6 = "buf-6";

    private static final String SAMPLE_JSON_LINE = """
            {"extract":{"extract_job_id":"job-1","extract_buffer_id":"buf-1","extract_type":"cdc"},"schema":{"type":"struct","fields":[]},"payload":{"before":{"id":1,"name":"scalar","value":1.06,"created_at":"2026-05-24T02:49:32.359424Z","updated_at":"2026-05-31T01:55:32.887469Z"},"after":{"id":1,"name":"scalar","value":1.04,"created_at":"2026-05-24T02:49:32.359424Z","updated_at":"2026-05-31T02:27:24.242870Z"},"source":{"version":"3.5.0.Final","connector":"postgresql","name":"extract","ts_ms":1780194444244,"snapshot":"false","db":"geo","sequence":"[\\"32589016\\",\\"32591512\\"]","ts_us":1780194444244620,"ts_ns":1780194444244620000,"schema":"public","table":"scalars","txId":22179,"lsn":32591512,"xmin":null,"origin":null,"origin_lsn":null},"transaction":null,"op":"u","ts_ms":1780194444583,"ts_us":1780194444583639,"ts_ns":1780194444583639448}}
            """;

    private static final String SAMPLE_JSON_LINE_V2 = """
            {"extract":{"extract_job_id":"job-1","extract_buffer_id":"buf-2","extract_type":"cdc"},"schema":{"type":"struct","fields":[]},"payload":{"before":{"id":1,"name":"scalar","value":1.04,"created_at":"2026-05-24T02:49:32.359424Z","updated_at":"2026-05-31T02:27:24.242870Z"},"after":{"id":1,"name":"scalar","value":1.02,"created_at":"2026-05-24T02:49:32.359424Z","updated_at":"2026-05-31T03:27:24.242870Z"},"source":{"version":"3.5.0.Final","connector":"postgresql","name":"extract","ts_ms":1780194445244,"snapshot":"false","db":"geo","sequence":"[\\"32592000\\",\\"32592512\\"]","ts_us":1780194445244620,"ts_ns":1780194445244620000,"schema":"public","table":"scalars","txId":22180,"lsn":32592000,"xmin":null,"origin":null,"origin_lsn":null},"transaction":null,"op":"u","ts_ms":1780194445583,"ts_us":1780194445583639,"ts_ns":1780194445583639448}}
            """;

    private static final String DELETE_JSON_LINE = """
            {"extract":{"extract_job_id":"job-1","extract_buffer_id":"buf-3","extract_type":"cdc"},"schema":{"type":"struct","fields":[]},"payload":{"before":{"id":1,"name":"scalar","value":1.04,"created_at":"2026-05-24T02:49:32.359424Z","updated_at":"2026-05-31T02:27:24.242870Z"},"after":null,"source":{"version":"3.5.0.Final","connector":"postgresql","name":"extract","ts_ms":1780198044244,"snapshot":"false","db":"geo","sequence":"[\\"32592900\\",\\"32593000\\"]","ts_us":1780198044244620,"ts_ns":1780198044244620000,"schema":"public","table":"scalars","txId":22185,"lsn":32593000,"xmin":null,"origin":null,"origin_lsn":null},"transaction":null,"op":"d","ts_ms":1780198044583,"ts_us":1780198044583639,"ts_ns":1780198044583639448}}
            """;

    private static final String DELETE_VALID_TO = "2026-05-31 03:27:24.583";

    private static final String TOMBSTONE_JSON_LINE = """
            {"extract":{"extract_job_id":"job-1","extract_buffer_id":"buf-4","extract_type":"cdc"},"schema":{"type":"struct","fields":[]},"payload":{"before":{"id":3,"name":"scalar","value":3.06,"created_at":"2026-05-24T02:49:32.359424Z","updated_at":"2026-05-31T01:55:32.887469Z"},"after":null,"source":{"version":"3.5.0.Final","connector":"postgresql","name":"extract","ts_ms":1780194444244,"snapshot":"false","db":"geo","sequence":"[\\"32589016\\",\\"32591512\\"]","ts_us":1780194444244620,"ts_ns":1780194444244620000,"schema":"public","table":"scalars","txId":22179,"lsn":32591512,"xmin":null,"origin":null,"origin_lsn":null},"transaction":null,"op":"d","ts_ms":1780194444583,"ts_us":1780194444583639,"ts_ns":1780194444583639448}}
            """;

    private static final String TOMBSTONE_VALID_TO = "2026-05-31 02:27:24.583";

    private SparkSession spark;

    @AfterEach
    void stopSpark() {
        if (spark != null) {
            spark.stop();
            SparkSession.clearActiveSession();
            SparkSession.clearDefaultSession();
            spark = null;
        }
    }

    @Test
    void transformReadsBronzeTableAfterCheckingSilverCatalog(@TempDir Path tempDir) throws Exception {
        TableIdentity table = writeBronzeTable(tempDir, SAMPLE_JSON_LINE);

        assertDoesNotThrow(() -> new Type2DimensionTransformer().transform(spark, table, BUFFER_1));
        assertEquals(1L, spark.table(table.getCatalogTableName()).count());
        assertTrue(spark.catalog().tableExists(toSilverCatalogTable(table)));
    }

    @Test
    void initialLoadCreatesType2Silver(@TempDir Path tempDir) throws Exception {
        TableIdentity table = writeBronzeTable(tempDir, SAMPLE_JSON_LINE);

        new Type2DimensionTransformer().transform(spark, table, BUFFER_1);

        Dataset<Row> silver = spark.table(toSilverCatalogTable(table));
        assertEquals(1L, silver.count());

        Row row = silver.first();
        assertEquals("1-32591512", row.getAs("primary_key"));
        assertNotNull(row.getAs("version_key"));
        assertFalse(((String) row.getAs("version_key")).isBlank());
        assertTrue(row.getBoolean(row.fieldIndex("_is_current")));
        assertEquals(
                END_OF_TIME_LITERAL,
                row.getTimestamp(row.fieldIndex("_valid_to")).toString().substring(0, END_OF_TIME_LITERAL.length()));
        assertEquals(row.getTimestamp(row.fieldIndex("updated_at")), row.getTimestamp(row.fieldIndex("_valid_from")));
        assertEquals("job-1", row.getAs("_extract_job_id"));
        assertEquals("buf-1", row.getAs("_extract_buffer_id"));
        assertEquals("cdc", row.getAs("_extract_type"));
    }

    @Test
    void incrementalLoadClosesPriorVersion(@TempDir Path tempDir) throws Exception {
        TableIdentity table = writeBronzeTable(tempDir, SAMPLE_JSON_LINE);
        new Type2DimensionTransformer().transform(spark, table, BUFFER_1);

        Path inputFile = tempDir.resolve("geo.public.scalars-2026-06-01_02-14-58.jsonl");
        Files.writeString(inputFile, SAMPLE_JSON_LINE_V2);
        new IcebergTableWriter().writeToTable(spark, inputFile, tempDir.resolve("bronze"));

        new Type2DimensionTransformer().transform(spark, table, BUFFER_2);

        Dataset<Row> silver = spark.table(toSilverCatalogTable(table));
        assertEquals(2L, silver.count());
        assertEquals(1L, silver.filter("_is_current = true").count());

        Row priorVersion = silver.filter("primary_key = '1-32591512'").first();
        Row currentVersion = silver.filter("primary_key = '1-32592000'").first();

        assertFalse(priorVersion.getBoolean(priorVersion.fieldIndex("_is_current")));
        assertTrue(currentVersion.getBoolean(currentVersion.fieldIndex("_is_current")));
        assertNotEquals(
                (String) priorVersion.getAs("version_key"), (String) currentVersion.getAs("version_key"));
        assertEquals(
                END_OF_TIME_LITERAL,
                currentVersion
                        .getTimestamp(currentVersion.fieldIndex("_valid_to"))
                        .toString()
                        .substring(0, END_OF_TIME_LITERAL.length()));
        assertEquals(
                currentVersion.getTimestamp(currentVersion.fieldIndex("updated_at")),
                currentVersion.getTimestamp(currentVersion.fieldIndex("_valid_from")));
        assertTrue(priorVersion.getTimestamp(priorVersion.fieldIndex("_valid_to"))
                .before(currentVersion.getTimestamp(currentVersion.fieldIndex("_valid_from"))));
    }

    @Test
    void refreshesExtractMetadataWhenUpdatedAtMatchesCurrentSilver(@TempDir Path tempDir) throws Exception {
        TableIdentity table = writeBronzeTable(tempDir, SAMPLE_JSON_LINE);
        new Type2DimensionTransformer().transform(spark, table, BUFFER_1);

        Row before = spark.table(toSilverCatalogTable(table)).first();
        String versionKey = before.getAs("version_key");
        assertEquals("1-32591512", before.getAs("primary_key"));
        assertEquals("buf-1", before.getAs("_extract_buffer_id"));
        assertEquals("cdc", before.getAs("_extract_type"));

        // Same id and updated_at as current silver, but a higher LSN (e.g. re-snapshot).
        String sameUpdatedAtHigherLsn = """
                {"extract":{"extract_job_id":"job-2","extract_buffer_id":"buf-6","extract_type":"snapshot"},"schema":{"type":"struct","fields":[]},"payload":{"before":null,"after":{"id":1,"name":"scalar","value":1.04,"created_at":"2026-05-24T02:49:32.359424Z","updated_at":"2026-05-31T02:27:24.242870Z"},"source":{"version":"3.5.0.Final","connector":"postgresql","name":"extract","ts_ms":1780194447244,"snapshot":"true","db":"geo","sequence":"[\\"32595000\\",\\"32595512\\"]","ts_us":1780194447244620,"ts_ns":1780194447244620000,"schema":"public","table":"scalars","txId":22182,"lsn":32595512,"xmin":null,"origin":null,"origin_lsn":null},"transaction":null,"op":"r","ts_ms":1780194447583,"ts_us":1780194447583639,"ts_ns":1780194447583639448}}
                """;
        Path inputFile = tempDir.resolve("geo.public.scalars-2026-06-02_00-00-00.jsonl");
        Files.writeString(inputFile, sameUpdatedAtHigherLsn);
        new IcebergTableWriter().writeToTable(spark, inputFile, tempDir.resolve("bronze"));

        new Type2DimensionTransformer().transform(spark, table, BUFFER_6);

        Dataset<Row> silver = spark.table(toSilverCatalogTable(table));
        assertEquals(1L, silver.count());
        Row after = silver.first();
        assertEquals("1-32591512", after.getAs("primary_key"));
        assertEquals(versionKey, after.getAs("version_key"));
        assertTrue(after.getBoolean(after.fieldIndex("_is_current")));
        assertEquals("job-2", after.getAs("_extract_job_id"));
        assertEquals("buf-6", after.getAs("_extract_buffer_id"));
        assertEquals("snapshot", after.getAs("_extract_type"));

        Row type1 = spark.table(toType1CatalogTable(table)).first();
        assertEquals("job-2", type1.getAs("_extract_job_id"));
        assertEquals("buf-6", type1.getAs("_extract_buffer_id"));
        assertEquals("snapshot", type1.getAs("_extract_type"));
        assertEquals("1-32591512", type1.getAs("primary_key"));
    }

    @Test
    void incrementalLoadIncludesRowsWithOlderUpdatedAtButHigherSourceLsn(@TempDir Path tempDir) throws Exception {
        TableIdentity table = writeBronzeTable(tempDir, SAMPLE_JSON_LINE);
        new Type2DimensionTransformer().transform(spark, table, BUFFER_1);

        // Business updated_at is older than silver's max updated_at, but source_lsn is higher.
        String olderUpdatedAtHigherLsn = """
                {"extract":{"extract_job_id":"job-1","extract_buffer_id":"buf-5","extract_type":"cdc"},"schema":{"type":"struct","fields":[]},"payload":{"before":null,"after":{"id":2,"name":"scalar","value":2.50,"created_at":"2026-05-20T01:00:00.000000Z","updated_at":"2026-05-30T01:00:00.000000Z"},"source":{"version":"3.5.0.Final","connector":"postgresql","name":"extract","ts_ms":1780194446244,"snapshot":"false","db":"geo","sequence":"[\\"32594000\\",\\"32594512\\"]","ts_us":1780194446244620,"ts_ns":1780194446244620000,"schema":"public","table":"scalars","txId":22181,"lsn":32594000,"xmin":null,"origin":null,"origin_lsn":null},"transaction":null,"op":"c","ts_ms":1780194446583,"ts_us":1780194446583639,"ts_ns":1780194446583639448}}
                """;
        Path inputFile = tempDir.resolve("geo.public.scalars-2026-06-01_04-00-00.jsonl");
        Files.writeString(inputFile, olderUpdatedAtHigherLsn);
        new IcebergTableWriter().writeToTable(spark, inputFile, tempDir.resolve("bronze"));

        new Type2DimensionTransformer().transform(spark, table, BUFFER_5);

        Dataset<Row> silver = spark.table(toSilverCatalogTable(table));
        assertEquals(2L, silver.count());
        assertEquals(1L, silver.filter("id = 2").count());
        assertEquals("2-32594000", silver.filter("id = 2").first().getAs("primary_key"));
    }

    @Test
    void normalUpsertDefaultsIsDeletedFalse(@TempDir Path tempDir) throws Exception {
        TableIdentity table = writeBronzeTable(tempDir, SAMPLE_JSON_LINE);
        new Type2DimensionTransformer().transform(spark, table, BUFFER_1);

        Row row = spark.table(toSilverCatalogTable(table)).first();
        assertFalse(row.getBoolean(row.fieldIndex("_is_deleted")));
    }

    @Test
    void deleteClosesExistingCurrentRow(@TempDir Path tempDir) throws Exception {
        TableIdentity table = writeBronzeTable(tempDir, SAMPLE_JSON_LINE);
        new Type2DimensionTransformer().transform(spark, table, BUFFER_1);

        String versionKeyBeforeDelete = spark.table(toSilverCatalogTable(table))
                .filter("primary_key = '1-32591512'")
                .first()
                .getAs("version_key");

        Path deleteFile = tempDir.resolve("geo.public.scalars-2026-06-01_03-27-58.jsonl");
        Files.writeString(deleteFile, DELETE_JSON_LINE);
        new IcebergTableWriter().writeToTable(spark, deleteFile, tempDir.resolve("bronze"));

        new Type2DimensionTransformer().transform(spark, table, BUFFER_3);

        Dataset<Row> silver = spark.table(toSilverCatalogTable(table));
        assertEquals(1L, silver.count());

        Row row = silver.first();
        assertEquals("1-32591512", row.getAs("primary_key"));
        assertEquals(versionKeyBeforeDelete, row.getAs("version_key"));
        assertTrue(row.getBoolean(row.fieldIndex("_is_deleted")));
        assertFalse(row.getBoolean(row.fieldIndex("_is_current")));
        assertEquals(DELETE_VALID_TO, row.getTimestamp(row.fieldIndex("_valid_to")).toString());
    }

    @Test
    void deleteWithoutPriorRowCreatesTombstone(@TempDir Path tempDir) throws Exception {
        TableIdentity table = writeBronzeTable(tempDir, TOMBSTONE_JSON_LINE);
        new Type2DimensionTransformer().transform(spark, table, BUFFER_4);

        Dataset<Row> silver = spark.table(toSilverCatalogTable(table));
        assertEquals(1L, silver.count());

        Row row = silver.first();
        assertEquals(3L, (long) row.getAs("id"));
        assertEquals("3-32591512", row.getAs("primary_key"));
        assertNotNull(row.getAs("version_key"));
        assertFalse(((String) row.getAs("version_key")).isBlank());
        assertTrue(row.getBoolean(row.fieldIndex("_is_deleted")));
        assertTrue(row.getBoolean(row.fieldIndex("_is_current")));
        assertEquals(TOMBSTONE_VALID_TO, row.getTimestamp(row.fieldIndex("_valid_to")).toString());
        assertEquals(
                "1970-01-01 00:00:00",
                row.getTimestamp(row.fieldIndex("_valid_from")).toString().substring(0, 19));
    }

    @Test
    void incrementalDeleteIsPickedUpAndIdempotent(@TempDir Path tempDir) throws Exception {
        TableIdentity table = writeBronzeTable(tempDir, SAMPLE_JSON_LINE);
        new Type2DimensionTransformer().transform(spark, table, BUFFER_1);

        Path deleteFile = tempDir.resolve("geo.public.scalars-2026-06-01_03-27-58.jsonl");
        Files.writeString(deleteFile, DELETE_JSON_LINE);
        new IcebergTableWriter().writeToTable(spark, deleteFile, tempDir.resolve("bronze"));

        new Type2DimensionTransformer().transform(spark, table, BUFFER_3);
        assertTrue(spark.table(toSilverCatalogTable(table)).filter("_is_deleted = true").count() == 1L);

        new Type2DimensionTransformer().transform(spark, table, BUFFER_3);

        Dataset<Row> silver = spark.table(toSilverCatalogTable(table));
        assertEquals(1L, silver.count());
        assertTrue(silver.first().getBoolean(silver.first().fieldIndex("_is_deleted")));
    }

    @Test
    void reprocessingSameBufferIsIdempotent(@TempDir Path tempDir) throws Exception {
        TableIdentity table = writeBronzeTable(tempDir, SAMPLE_JSON_LINE);
        Type2DimensionTransformer transformer = new Type2DimensionTransformer();

        transformer.transform(spark, table, BUFFER_1);
        Dataset<Row> afterFirst = spark.table(toSilverCatalogTable(table));
        assertEquals(1L, afterFirst.count());
        String versionKey = afterFirst.first().getAs("version_key");

        transformer.transform(spark, table, BUFFER_1);

        Dataset<Row> afterSecond = spark.table(toSilverCatalogTable(table));
        assertEquals(1L, afterSecond.count());
        assertEquals(versionKey, afterSecond.first().getAs("version_key"));
        assertTrue(afterSecond.first().getBoolean(afterSecond.first().fieldIndex("_is_current")));
    }

    @Test
    void transformThrowsWhenNoRowsForBuffer(@TempDir Path tempDir) throws Exception {
        TableIdentity table = writeBronzeTable(tempDir, SAMPLE_JSON_LINE);

        RuntimeException thrown = assertThrows(
                RuntimeException.class,
                () -> new Type2DimensionTransformer().transform(spark, table, "missing-buffer"));
        Throwable root = thrown;
        while (root.getCause() != null) {
            root = root.getCause();
        }
        assertTrue(root instanceof IllegalStateException);
        assertTrue(root.getMessage().contains("missing-buffer"));
        assertFalse(spark.catalog().tableExists(toSilverCatalogTable(table)));
    }

    @Test
    void transformSkipsWhenExtractBufferIdMissing(@TempDir Path tempDir) throws Exception {
        TableIdentity table = writeBronzeTable(tempDir, SAMPLE_JSON_LINE);

        assertDoesNotThrow(() -> new Type2DimensionTransformer()
                .transform(spark, new IcebergType2TableAccess(table, tempDir.resolve("silver")), null, null, null));
        assertFalse(spark.catalog().tableExists(toSilverCatalogTable(table)));
    }

    @Test
    void sequentialBuffersInSameSessionBothReachSilver(@TempDir Path tempDir) throws Exception {
        TableIdentity table = writeBronzeTable(tempDir, SAMPLE_JSON_LINE);
        Type2DimensionTransformer transformer = new Type2DimensionTransformer();
        transformer.transform(spark, table, BUFFER_1);

        Path inputFile = tempDir.resolve("geo.public.scalars-2026-06-01_02-14-58.jsonl");
        Files.writeString(inputFile, SAMPLE_JSON_LINE_V2);
        new IcebergTableWriter().writeToTable(spark, inputFile, tempDir.resolve("bronze"));

        // Same Spark session must see the newly appended buffer after catalog refresh.
        transformer.transform(spark, table, BUFFER_2);

        Dataset<Row> silver = spark.table(toSilverCatalogTable(table));
        assertEquals(2L, silver.count());
        assertEquals(1L, silver.filter("_extract_buffer_id = 'buf-1'").count());
        assertEquals(1L, silver.filter("_extract_buffer_id = 'buf-2'").count());
        assertEquals(1L, silver.filter("_is_current = true").count());
    }

    @Test
    void transformRegistersType2WriteForIceberg(@TempDir Path tempDir) throws Exception {
        TableIdentity table = writeBronzeTable(tempDir, SAMPLE_JSON_LINE);
        RecordingDatapipelinesClient client = new RecordingDatapipelinesClient();
        KafkaWriteContext kafka = new KafkaWriteContext("open-table-write-notifications", 0, 7L);
        Type2WriteIdentity type2WriteIdentity = new Type2WriteIdentity(
                "test-instance",
                "geo",
                "public",
                "scalars",
                "geo",
                "public_silver",
                "scalars_type2",
                "/opt/data/raw/geo/public/scalars/file.jsonl",
                2048L,
                "job-1",
                BUFFER_1,
                "cdc",
                null,
                null,
                null,
                null,
                null,
                null);
        Type1WriteIdentity type1WriteIdentity = new Type1WriteIdentity(
                "test-instance",
                "geo",
                "public",
                "scalars",
                "geo",
                "public_silver",
                "scalars_type1",
                "/opt/data/raw/geo/public/scalars/file.jsonl",
                2048L,
                "job-1",
                BUFFER_1,
                "cdc",
                null,
                null,
                null,
                null,
                null,
                null);

        new Type2DimensionTransformer(Observability.noop(), client)
                .transform(
                        spark,
                        new IcebergType2TableAccess(table, tempDir.resolve("silver")),
                        kafka,
                        type2WriteIdentity,
                        type1WriteIdentity);

        assertEquals(1, client.type2Writes().size());
        assertEquals(1, client.type1Writes().size());
        assertEquals(0, client.bronzeWrites().size());
        TableWriteRegistration registration = client.type2Writes().get(0);
        assertEquals(TableWriteRegistration.WRITE_TYPE_SILVER_TYPE_2, registration.writeType());
        assertEquals("test-instance", registration.sourceInstanceName());
        assertEquals("geo", registration.sourceDatabaseName());
        assertEquals("public", registration.sourceSchemaName());
        assertEquals("scalars", registration.sourceTableName());
        assertEquals("geo", registration.databaseName());
        assertEquals("public_silver", registration.namespaceName());
        assertEquals("scalars_type2", registration.tableName());
        assertEquals(1L, registration.writeRowCount());
        assertEquals(1L, registration.mergeRowCount());
        assertEquals("/opt/data/raw/geo/public/scalars/file.jsonl", registration.rawFilePath());
        assertEquals(2048L, registration.rawFileSize());
        assertEquals("job-1", registration.extractJobId());
        assertEquals(BUFFER_1, registration.extractBufferId());
        assertNotNull(registration.mergeStartAt());
        assertNotNull(registration.mergeEndAt());
        assertTrue(!registration.mergeStartAt().isAfter(registration.mergeEndAt()));
        assertEquals(tempDir.resolve("silver").toAbsolutePath().toString(), registration.warehousePath());
        assertEquals(kafka, registration.kafka());

        TableWriteRegistration type1Registration = client.type1Writes().get(0);
        assertEquals(TableWriteRegistration.WRITE_TYPE_SILVER_TYPE_1, type1Registration.writeType());
        assertEquals("scalars_type1", type1Registration.tableName());
        assertEquals("public_silver", type1Registration.namespaceName());
        assertEquals(1L, type1Registration.writeRowCount());
        assertEquals(1L, type1Registration.mergeRowCount());
        assertEquals("/opt/data/raw/geo/public/scalars/file.jsonl", type1Registration.rawFilePath());
        assertEquals(2048L, type1Registration.rawFileSize());
        assertNotNull(type1Registration.mergeStartAt());
        assertNotNull(type1Registration.mergeEndAt());
        assertEquals(kafka, type1Registration.kafka());
    }

    @Test
    void transformContinuesWhenDatapipelinesRegistrationFails(@TempDir Path tempDir) throws Exception {
        TableIdentity table = writeBronzeTable(tempDir, SAMPLE_JSON_LINE);
        RecordingDatapipelinesClient client = new RecordingDatapipelinesClient();
        client.failPosts();
        Type2WriteIdentity type2WriteIdentity = new Type2WriteIdentity(
                "test-instance",
                "geo",
                "public",
                "scalars",
                "geo",
                "public_silver",
                "scalars_type2",
                null,
                null,
                null,
                BUFFER_1,
                null,
                null,
                null,
                null,
                null,
                null,
                null);
        Type1WriteIdentity type1WriteIdentity = new Type1WriteIdentity(
                "test-instance",
                "geo",
                "public",
                "scalars",
                "geo",
                "public_silver",
                "scalars_type1",
                null,
                null,
                null,
                BUFFER_1,
                null,
                null,
                null,
                null,
                null,
                null,
                null);

        assertDoesNotThrow(() -> new Type2DimensionTransformer(Observability.noop(), client)
                .transform(
                        spark,
                        new IcebergType2TableAccess(table, tempDir.resolve("silver")),
                        null,
                        type2WriteIdentity,
                        type1WriteIdentity));
        assertTrue(spark.catalog().tableExists(toSilverCatalogTable(table)));
        assertTrue(spark.catalog().tableExists(toType1CatalogTable(table)));
        assertEquals(0, client.type2Writes().size());
        assertEquals(0, client.type1Writes().size());
    }

    @Test
    void initialLoadCreatesType1Silver(@TempDir Path tempDir) throws Exception {
        TableIdentity table = writeBronzeTable(tempDir, SAMPLE_JSON_LINE);

        new Type2DimensionTransformer().transform(spark, table, BUFFER_1);

        Dataset<Row> type1 = spark.table(toType1CatalogTable(table));
        assertEquals(1L, type1.count());

        Row row = type1.first();
        assertEquals(1L, (long) row.getAs("id"));
        assertEquals("1-32591512", row.getAs("primary_key"));
        assertFalse(row.getBoolean(row.fieldIndex("_is_deleted")));
        assertFalse(Arrays.asList(type1.columns()).contains("_is_current"));
        assertFalse(Arrays.asList(type1.columns()).contains("_valid_from"));
        assertFalse(Arrays.asList(type1.columns()).contains("_valid_to"));
    }

    @Test
    void incrementalLoadUpdatesType1InPlace(@TempDir Path tempDir) throws Exception {
        TableIdentity table = writeBronzeTable(tempDir, SAMPLE_JSON_LINE);
        new Type2DimensionTransformer().transform(spark, table, BUFFER_1);

        Path inputFile = tempDir.resolve("geo.public.scalars-2026-06-01_02-14-58.jsonl");
        Files.writeString(inputFile, SAMPLE_JSON_LINE_V2);
        new IcebergTableWriter().writeToTable(spark, inputFile, tempDir.resolve("bronze"));

        new Type2DimensionTransformer().transform(spark, table, BUFFER_2);

        Dataset<Row> type1 = spark.table(toType1CatalogTable(table));
        assertEquals(1L, type1.count());
        Row row = type1.first();
        assertEquals("1-32592000", row.getAs("primary_key"));
        assertEquals(1.02, ((Number) row.getAs("value")).doubleValue(), 0.0001);
        assertFalse(row.getBoolean(row.fieldIndex("_is_deleted")));
    }

    @Test
    void deleteSoftDeletesType1Row(@TempDir Path tempDir) throws Exception {
        TableIdentity table = writeBronzeTable(tempDir, SAMPLE_JSON_LINE);
        new Type2DimensionTransformer().transform(spark, table, BUFFER_1);

        Path deleteFile = tempDir.resolve("geo.public.scalars-2026-06-01_03-27-58.jsonl");
        Files.writeString(deleteFile, DELETE_JSON_LINE);
        new IcebergTableWriter().writeToTable(spark, deleteFile, tempDir.resolve("bronze"));

        new Type2DimensionTransformer().transform(spark, table, BUFFER_3);

        Dataset<Row> type1 = spark.table(toType1CatalogTable(table));
        assertEquals(1L, type1.count());
        Row row = type1.first();
        assertEquals(1L, (long) row.getAs("id"));
        assertTrue(row.getBoolean(row.fieldIndex("_is_deleted")));
    }

    @Test
    void tombstoneCreatesDeletedType1Row(@TempDir Path tempDir) throws Exception {
        TableIdentity table = writeBronzeTable(tempDir, TOMBSTONE_JSON_LINE);
        new Type2DimensionTransformer().transform(spark, table, BUFFER_4);

        Dataset<Row> type1 = spark.table(toType1CatalogTable(table));
        assertEquals(1L, type1.count());
        Row row = type1.first();
        assertEquals(3L, (long) row.getAs("id"));
        assertEquals("3-32591512", row.getAs("primary_key"));
        assertTrue(row.getBoolean(row.fieldIndex("_is_deleted")));
    }

    private TableIdentity writeBronzeTable(Path tempDir, String jsonLine) throws Exception {
        Path bronzeWarehouse = tempDir.resolve("bronze");
        Path silverWarehouse = tempDir.resolve("silver");
        Path inputFile = tempDir.resolve("geo.public.scalars-2026-05-31_02-51-21.jsonl");
        Files.writeString(inputFile, jsonLine);

        spark = new SparkSessionFactory().createIcebergTableSparkSession(bronzeWarehouse, silverWarehouse);
        new IcebergTableWriter().writeToTable(spark, inputFile, bronzeWarehouse);

        Path sourceTablePath = bronzeWarehouse.resolve("geo").resolve("public_bronze").resolve("scalars");
        return IcebergTableIdentity.fromTablePath(sourceTablePath);
    }

    private static String toSilverCatalogTable(TableIdentity table) {
        String[] parts = table.getTableFqn().split("\\.");
        return String.format(
                "silver_catalog.%s.%s.%s",
                parts[0],
                OpenTableNamespaces.silverFromBronze(parts[1]),
                OpenTableNamespaces.type2Table(parts[2]));
    }

    private static String toType1CatalogTable(TableIdentity table) {
        String[] parts = table.getTableFqn().split("\\.");
        return String.format(
                "silver_catalog.%s.%s.%s",
                parts[0],
                OpenTableNamespaces.silverFromBronze(parts[1]),
                OpenTableNamespaces.type1Table(parts[2]));
    }
}
