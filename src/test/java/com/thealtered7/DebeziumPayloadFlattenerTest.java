package com.thealtered7;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DebeziumPayloadFlattenerTest {

    private static SparkSession spark;

    @BeforeAll
    static void startSpark() {
        spark = SparkSession.builder()
                .appName("DebeziumPayloadFlattenerTest")
                .master("local[*]")
                .config("spark.ui.enabled", "false")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");
    }

    @AfterAll
    static void stopSpark() {
        if (spark != null) {
            spark.stop();
        }
    }

    @Test
    void flattenPayloadAndConvertTimestamps(@TempDir Path tempDir) throws Exception {
        String jsonLine = """
                {"schema":{"type":"struct","fields":[]},"payload":{"before":{"id":1,"name":"scalar","value":1.06,"created_at":"2026-05-24T02:49:32.359424Z","updated_at":"2026-05-31T01:55:32.887469Z"},"after":{"id":1,"name":"scalar","value":1.04,"created_at":"2026-05-24T02:49:32.359424Z","updated_at":"2026-05-31T02:27:24.242870Z"},"source":{"version":"3.5.0.Final","connector":"postgresql","name":"extract","ts_ms":1780194444244,"snapshot":"false","db":"geo","sequence":"[\\"32589016\\",\\"32591512\\"]","ts_us":1780194444244620,"ts_ns":1780194444244620000,"schema":"public","table":"scalars","txId":22179,"lsn":32591512,"xmin":null,"origin":null,"origin_lsn":null},"transaction":null,"op":"u","ts_ms":1780194444583,"ts_us":1780194444583639,"ts_ns":1780194444583639448}}
                """;
        Path inputFile = tempDir.resolve("geo.public.scalars-2026-05-31_02-51-21.jsonl");
        Files.writeString(inputFile, jsonLine);
        DebeziumPayloadFlattener flattener = new DebeziumPayloadFlattener();
        Dataset<Row> raw = flattener.loadJsonLines(spark, inputFile);
        Dataset<Row> flat = flattener.flattenPayload(raw);
        Dataset<Row> withTimestamps = flattener.convertTimestampColumns(flat);

        List<String> columnNames = Arrays.asList(withTimestamps.columns());
        assertTrue(columnNames.contains("before_id"));
        assertTrue(columnNames.contains("after_id"));
        assertTrue(columnNames.contains("source_db"));
        assertTrue(columnNames.contains("op"));
        assertTrue(columnNames.contains("ts_ms"));

        assertEquals(DataTypes.TimestampType, withTimestamps.schema().apply("before_created_at").dataType());
        assertEquals(1L, withTimestamps.count());
    }

    @Test
    void flattenPayloadHandlesInsertWithNullBefore(@TempDir Path tempDir) throws Exception {
        String jsonLine = """
                {"schema":{"type":"struct","fields":[]},"payload":{"before":null,"after":{"id":2,"name":"scalar","value":2.04,"created_at":"2026-05-24T02:49:32.359424Z","updated_at":"2026-05-31T02:27:24.242870Z"},"source":{"version":"3.5.0.Final","connector":"postgresql","name":"extract","ts_ms":1780194444244,"snapshot":"false","db":"geo","sequence":"[\\"32589016\\",\\"32591512\\"]","ts_us":1780194444244620,"ts_ns":1780194444244620000,"schema":"public","table":"scalars","txId":22179,"lsn":32591512,"xmin":null,"origin":null,"origin_lsn":null},"transaction":null,"op":"c","ts_ms":1780194444583,"ts_us":1780194444583639,"ts_ns":1780194444583639448}}
                """;
        Path inputFile = tempDir.resolve("geo.public.scalars-2026-05-31_02-51-21.jsonl");
        Files.writeString(inputFile, jsonLine);
        DebeziumPayloadFlattener flattener = new DebeziumPayloadFlattener();
        Dataset<Row> raw = flattener.loadJsonLines(spark, inputFile);
        Dataset<Row> flat = flattener.flattenPayload(raw);

        List<String> columnNames = Arrays.asList(flat.columns());
        assertTrue(columnNames.contains("after_id"));
        assertTrue(columnNames.contains("before_id"));
        assertTrue(columnNames.contains("source_db"));
        assertTrue(columnNames.contains("op"));

        Row row = flat.first();
        assertTrue(row.isNullAt(flat.schema().fieldIndex("before_id")));
        assertEquals(2L, (long) row.getAs("after_id"));
    }

    @Test
    void flattenPayloadHandlesDeleteWithNullAfter(@TempDir Path tempDir) throws Exception {
        String jsonLine = """
                {"schema":{"type":"struct","fields":[]},"payload":{"before":{"id":3,"name":"scalar","value":3.06,"created_at":"2026-05-24T02:49:32.359424Z","updated_at":"2026-05-31T01:55:32.887469Z"},"after":null,"source":{"version":"3.5.0.Final","connector":"postgresql","name":"extract","ts_ms":1780194444244,"snapshot":"false","db":"geo","sequence":"[\\"32589016\\",\\"32591512\\"]","ts_us":1780194444244620,"ts_ns":1780194444244620000,"schema":"public","table":"scalars","txId":22179,"lsn":32591512,"xmin":null,"origin":null,"origin_lsn":null},"transaction":null,"op":"d","ts_ms":1780194444583,"ts_us":1780194444583639,"ts_ns":1780194444583639448}}
                """;
        Path inputFile = tempDir.resolve("geo.public.scalars-2026-05-31_02-52-21.jsonl");
        Files.writeString(inputFile, jsonLine);
        DebeziumPayloadFlattener flattener = new DebeziumPayloadFlattener();
        Dataset<Row> raw = flattener.loadJsonLines(spark, inputFile);
        Dataset<Row> flat = flattener.flattenPayload(raw);

        List<String> columnNames = Arrays.asList(flat.columns());
        assertTrue(columnNames.contains("before_id"));
        assertTrue(columnNames.contains("after_id"));
        assertTrue(columnNames.contains("source_db"));
        assertTrue(columnNames.contains("op"));

        Row row = flat.first();
        assertTrue(row.isNullAt(flat.schema().fieldIndex("after_id")));
        assertEquals(3L, (long) row.getAs("before_id"));
    }

    @Test
    void getOutputTablePathBuildsTablePath() {
        DebeziumPayloadFlattener flattener = new DebeziumPayloadFlattener();
        Path base = Path.of("/opt/data/iceberg");
        Path result = flattener.getOutputTablePath("geo.public.scalars", base);

        Path expected = Path.of("/opt/data/iceberg/geo/public/scalars");
        assertEquals(expected, result);
    }
}
