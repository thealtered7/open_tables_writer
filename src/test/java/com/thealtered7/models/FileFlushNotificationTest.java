package com.thealtered7.models;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FileFlushNotificationTest {

    private static final ObjectMapper OBJECT_MAPPER = FileFlushNotificationJson.MAPPER;

    private static final String RAW_FILE_PATH =
            "/opt/data/raw/geo/public/scalars/2026/05/31/geo.public.scalars-2026-05-31_02-51-21.jsonl";
    private static final String TABLE_NAME = "geo.public.scalars";
    private static final String EXTRACT_JOB_ID = "abc-123";
    private static final String EXTRACT_BUFFER_ID = "buf-1";
    private static final Instant EXTRACT_START_AT = Instant.parse("2026-05-31T02:50:21.544690220Z");
    private static final Instant EXTRACT_END_AT = Instant.parse("2026-05-31T02:51:21.544690220Z");
    private static final String SOURCE_INSTANCE = "test-instance";
    private static final String SOURCE_DATABASE = "geo";
    private static final String SOURCE_SCHEMA = "public";
    private static final String SOURCE_TABLE = "scalars";
    private static final String WAREHOUSE_PATH = "/opt/data/raw/geo/public/scalars/";

    @Test
    void serializesWithExpectedJsonPropertyNames() throws Exception {
        FileFlushNotification notification = notification();

        JsonNode json = OBJECT_MAPPER.readTree(OBJECT_MAPPER.writeValueAsString(notification));

        assertEquals("raw", json.get("write_type").asText());
        assertEquals(RAW_FILE_PATH, json.get("raw_file_path").asText());
        assertEquals(TABLE_NAME, json.get("table_name").asText());
        assertEquals(EXTRACT_JOB_ID, json.get("extract_job_id").asText());
        assertEquals(EXTRACT_BUFFER_ID, json.get("extract_buffer_id").asText());
        assertEquals("cdc", json.get("extract_type").asText());
        assertEquals(EXTRACT_START_AT.toString(), json.get("extract_start_at").asText());
        assertEquals(EXTRACT_END_AT.toString(), json.get("extract_end_at").asText());
        assertEquals(2L, json.get("write_row_count").asLong());
        assertEquals(42L, json.get("raw_file_size").asLong());
        assertEquals(SOURCE_INSTANCE, json.get("source_instance_name").asText());
        assertEquals(SOURCE_DATABASE, json.get("source_database_name").asText());
        assertEquals(SOURCE_SCHEMA, json.get("source_schema_name").asText());
        assertEquals(SOURCE_TABLE, json.get("source_table_name").asText());
        assertEquals(WAREHOUSE_PATH, json.get("warehouse_path").asText());
        assertEquals(true, json.get("key_schema").isNull());
        assertEquals(true, json.get("value_schema").isNull());
        assertEquals(true, json.get("key_schema_id").isNull());
        assertEquals(true, json.get("value_schema_id").isNull());
        assertEquals(32591512L, json.get("source_min_lsn").asLong());
        assertEquals(32592000L, json.get("source_max_lsn").asLong());
    }

    @Test
    void deserializesPgoutputJsonFormat() throws Exception {
        String json =
                """
                {"write_type":"raw",\
                "raw_file_path":"/opt/data/raw/geo/public/scalars/2026/05/31/geo.public.scalars-2026-05-31_02-51-21.jsonl",\
                "table_name":"geo.public.scalars","extract_job_id":"abc-123",\
                "extract_buffer_id":"buf-1","extract_type":"cdc",\
                "extract_start_at":"2026-05-31T02:50:21.544690220Z",\
                "extract_end_at":"2026-05-31T02:51:21.544690220Z",\
                "write_row_count":2,"raw_file_size":42,\
                "source_instance_name":"test-instance","source_database_name":"geo",\
                "source_schema_name":"public","source_table_name":"scalars",\
                "warehouse_path":"/opt/data/raw/geo/public/scalars/"}
                """;

        FileFlushNotification notification = OBJECT_MAPPER.readValue(json, FileFlushNotification.class);

        assertEquals(RAW_FILE_PATH, notification.rawFilePath());
        assertEquals(TABLE_NAME, notification.tableName());
        assertEquals(EXTRACT_JOB_ID, notification.extractJobId());
        assertEquals(EXTRACT_END_AT, notification.extractEndAt());
        assertEquals(SOURCE_INSTANCE, notification.sourceInstanceName());
        assertEquals(SOURCE_DATABASE, notification.sourceDatabaseName());
        assertEquals(SOURCE_SCHEMA, notification.sourceSchemaName());
        assertEquals(SOURCE_TABLE, notification.sourceTableName());
        assertEquals(WAREHOUSE_PATH, notification.warehousePath());
    }

    @Test
    void roundTripPreservesAllFields() throws Exception {
        FileFlushNotification original = notification();

        String json = OBJECT_MAPPER.writeValueAsString(original);
        FileFlushNotification restored = OBJECT_MAPPER.readValue(json, FileFlushNotification.class);

        assertEquals(original, restored);
    }

    private static FileFlushNotification notification() {
        return new FileFlushNotification(
                "raw",
                RAW_FILE_PATH,
                TABLE_NAME,
                EXTRACT_JOB_ID,
                EXTRACT_BUFFER_ID,
                "cdc",
                EXTRACT_START_AT,
                EXTRACT_END_AT,
                2L,
                42L,
                SOURCE_INSTANCE,
                SOURCE_DATABASE,
                SOURCE_SCHEMA,
                SOURCE_TABLE,
                WAREHOUSE_PATH,
                null,
                null,
                null,
                null,
                32591512L,
                32592000L);
    }
}
