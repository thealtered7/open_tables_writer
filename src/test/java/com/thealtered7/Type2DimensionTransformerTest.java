package com.thealtered7;

import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class Type2DimensionTransformerTest {

    private static final String END_OF_TIME_LITERAL = "10000-12-31 23:59:59.999999";

    private static final String SAMPLE_JSON_LINE = """
            {"schema":{"type":"struct","fields":[]},"payload":{"before":{"id":1,"name":"scalar","value":1.06,"created_at":"2026-05-24T02:49:32.359424Z","updated_at":"2026-05-31T01:55:32.887469Z"},"after":{"id":1,"name":"scalar","value":1.04,"created_at":"2026-05-24T02:49:32.359424Z","updated_at":"2026-05-31T02:27:24.242870Z"},"source":{"version":"3.5.0.Final","connector":"postgresql","name":"extract","ts_ms":1780194444244,"snapshot":"false","db":"geo","sequence":"[\\"32589016\\",\\"32591512\\"]","ts_us":1780194444244620,"ts_ns":1780194444244620000,"schema":"public","table":"scalars","txId":22179,"lsn":32591512,"xmin":null,"origin":null,"origin_lsn":null},"transaction":null,"op":"u","ts_ms":1780194444583,"ts_us":1780194444583639,"ts_ns":1780194444583639448}}
            """;

    private static final String SAMPLE_JSON_LINE_V2 = """
            {"schema":{"type":"struct","fields":[]},"payload":{"before":{"id":1,"name":"scalar","value":1.04,"created_at":"2026-05-24T02:49:32.359424Z","updated_at":"2026-05-31T02:27:24.242870Z"},"after":{"id":1,"name":"scalar","value":1.02,"created_at":"2026-05-24T02:49:32.359424Z","updated_at":"2026-05-31T03:27:24.242870Z"},"source":{"version":"3.5.0.Final","connector":"postgresql","name":"extract","ts_ms":1780194445244,"snapshot":"false","db":"geo","sequence":"[\\"32592000\\",\\"32592512\\"]","ts_us":1780194445244620,"ts_ns":1780194445244620000,"schema":"public","table":"scalars","txId":22180,"lsn":32592000,"xmin":null,"origin":null,"origin_lsn":null},"transaction":null,"op":"u","ts_ms":1780194445583,"ts_us":1780194445583639,"ts_ns":1780194445583639448}}
            """;

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

        assertDoesNotThrow(() -> new Type2DimensionTransformer().transform(spark, table));
        assertEquals(1L, spark.table(table.getCatalogTableName()).count());
        assertTrue(spark.catalog().tableExists(toSilverCatalogTable(table)));
    }

    @Test
    void initialLoadCreatesType2Silver(@TempDir Path tempDir) throws Exception {
        TableIdentity table = writeBronzeTable(tempDir, SAMPLE_JSON_LINE);

        new Type2DimensionTransformer().transform(spark, table);

        Dataset<Row> silver = spark.table(toSilverCatalogTable(table));
        assertEquals(1L, silver.count());

        Row row = silver.first();
        assertEquals("1-32591512", row.getAs("primary_key"));
        assertTrue(row.getBoolean(row.fieldIndex("is_current")));
        assertEquals(
                END_OF_TIME_LITERAL,
                row.getTimestamp(row.fieldIndex("valid_to")).toString().substring(0, END_OF_TIME_LITERAL.length()));
        assertEquals(row.getTimestamp(row.fieldIndex("updated_at")), row.getTimestamp(row.fieldIndex("valid_from")));
    }

    @Test
    void incrementalLoadClosesPriorVersion(@TempDir Path tempDir) throws Exception {
        TableIdentity table = writeBronzeTable(tempDir, SAMPLE_JSON_LINE);
        new Type2DimensionTransformer().transform(spark, table);

        Path inputFile = tempDir.resolve("geo.public.scalars-2026-06-01_02-14-58.jsonl");
        Files.writeString(inputFile, SAMPLE_JSON_LINE_V2);
        new IcebergTableWriter().writeToTable(spark, inputFile, tempDir.resolve("bronze"));

        new Type2DimensionTransformer().transform(spark, table);

        Dataset<Row> silver = spark.table(toSilverCatalogTable(table));
        assertEquals(2L, silver.count());
        assertEquals(1L, silver.filter("is_current = true").count());

        Row priorVersion = silver.filter("primary_key = '1-32591512'").first();
        Row currentVersion = silver.filter("primary_key = '1-32592000'").first();

        assertFalse(priorVersion.getBoolean(priorVersion.fieldIndex("is_current")));
        assertTrue(currentVersion.getBoolean(currentVersion.fieldIndex("is_current")));
        assertEquals(
                END_OF_TIME_LITERAL,
                currentVersion
                        .getTimestamp(currentVersion.fieldIndex("valid_to"))
                        .toString()
                        .substring(0, END_OF_TIME_LITERAL.length()));
        assertEquals(
                currentVersion.getTimestamp(currentVersion.fieldIndex("updated_at")),
                currentVersion.getTimestamp(currentVersion.fieldIndex("valid_from")));
        assertTrue(priorVersion.getTimestamp(priorVersion.fieldIndex("valid_to"))
                .before(currentVersion.getTimestamp(currentVersion.fieldIndex("valid_from"))));
    }

    private TableIdentity writeBronzeTable(Path tempDir, String jsonLine) throws Exception {
        Path bronzeWarehouse = tempDir.resolve("bronze");
        Path silverWarehouse = tempDir.resolve("silver");
        Path inputFile = tempDir.resolve("geo.public.scalars-2026-05-31_02-51-21.jsonl");
        Files.writeString(inputFile, jsonLine);

        spark = new SparkSessionFactory().createIcebergTableSparkSession(bronzeWarehouse, silverWarehouse);
        new IcebergTableWriter().writeToTable(spark, inputFile, bronzeWarehouse);

        Path sourceTablePath = bronzeWarehouse.resolve("geo").resolve("public").resolve("scalars");
        return IcebergTableIdentity.fromTablePath(sourceTablePath);
    }

    private static String toSilverCatalogTable(TableIdentity table) {
        return table.getCatalogTableName().replace("local_catalog.", "silver_catalog.");
    }
}
