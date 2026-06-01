package com.thealtered7;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DeltaTableWriterTest {

    private static final String SAMPLE_JSON_LINE = """
            {"schema":{"type":"struct","fields":[]},"payload":{"before":{"id":1,"name":"scalar","value":1.06,"created_at":"2026-05-24T02:49:32.359424Z","updated_at":"2026-05-31T01:55:32.887469Z"},"after":{"id":1,"name":"scalar","value":1.04,"created_at":"2026-05-24T02:49:32.359424Z","updated_at":"2026-05-31T02:27:24.242870Z"},"source":{"version":"3.5.0.Final","connector":"postgresql","name":"extract","ts_ms":1780194444244,"snapshot":"false","db":"geo","sequence":"[\\"32589016\\",\\"32591512\\"]","ts_us":1780194444244620,"ts_ns":1780194444244620000,"schema":"public","table":"scalars","txId":22179,"lsn":32591512,"xmin":null,"origin":null,"origin_lsn":null},"transaction":null,"op":"u","ts_ms":1780194444583,"ts_us":1780194444583639,"ts_ns":1780194444583639448}}
            """;

    private static SparkSession spark;

    @BeforeAll
    static void startSpark() {
        spark = new SparkSessionFactory()
                .createDeltaTableSparkSession(Path.of("build/test-delta-warehouse"));
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
        DeltaTableWriter writer = new DeltaTableWriter();
        Path inputFile = Path.of("geo.public.scalars-2026-05-31_02-51-21.jsonl");

        assertEquals("geo.public.scalars", invokeGetTableFqn(writer, inputFile));
    }

    @Test
    void getTableFqnStripsJsonlSuffixWhenPatternDoesNotMatch() {
        DeltaTableWriter writer = new DeltaTableWriter();
        Path inputFile = Path.of("custom.table.jsonl");

        assertEquals("custom.table", invokeGetTableFqn(writer, inputFile));
    }

    @Test
    void writeToTableCreatesThenAppendsDeltaTable(@TempDir Path tempDir) throws Exception {
        Path inputFile = tempDir.resolve("geo.public.scalars-2026-05-31_02-51-21.jsonl");
        Files.writeString(inputFile, SAMPLE_JSON_LINE);
        Path dataDirectory = tempDir.resolve("warehouse");

        DeltaTableWriter writer = new DeltaTableWriter();
        writer.writeToTable(spark, inputFile, dataDirectory);

        Path outputTablePath = new DebeziumPayloadFlattener()
                .getOutputTablePath("geo.public.scalars", dataDirectory);

        Dataset<Row> created = spark.read().format("delta").load(outputTablePath.toString());
        assertEquals(1L, created.count());

        Files.writeString(inputFile, SAMPLE_JSON_LINE);
        writer.writeToTable(spark, inputFile, dataDirectory);

        Dataset<Row> appended = spark.read().format("delta").load(outputTablePath.toString());
        assertEquals(2L, appended.count());
        assertTrue(new DeltaTableOperations().tableExists(spark, outputTablePath));
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
        assertTrue(columns.contains("source_db"));
        assertTrue(columns.contains("op"));

        DeltaTableWriter writer = new DeltaTableWriter();
        writer.writeToTable(spark, inputFile, dataDirectory);
    }

    @Test
    void runRequiresInputFilePathSystemProperty() {
        System.clearProperty("input.file.path");
        System.clearProperty("data.directory.base.path");

        assertThrowsExactly(NullPointerException.class, () -> new DeltaTableWriter().run());
    }

    private static String invokeGetTableFqn(DeltaTableWriter writer, Path inputFile) {
        try {
            var method = DeltaTableWriter.class.getDeclaredMethod("getTableFqn", Path.class);
            method.setAccessible(true);
            return (String) method.invoke(writer, inputFile);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

}
