package com.thealtered7.models;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.thealtered7.datapipelines.TableWriteRegistration;
import com.thealtered7.models.TableUpdatedNotification.OpenTableFormat;
import java.time.Instant;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TableUpdatedNotificationTest {

    private static final ObjectMapper OBJECT_MAPPER = TableUpdatedNotificationJson.MAPPER;

    private static final String WRITE_TYPE = TableWriteRegistration.WRITE_TYPE_BRONZE;
    private static final String TABLE_FQN = "geo.public_bronze.scalars";
    private static final String TABLE_PATH = "/opt/data/iceberg/geo/public_bronze/scalars";
    private static final OpenTableFormat FORMAT = OpenTableFormat.ICEBERG;
    private static final String CATALOG = "lakehouse";
    private static final String DATABASE = "geo";
    private static final String NAMESPACE = "public_bronze";
    private static final String TABLE = "scalars";
    private static final String WAREHOUSE_PATH = "/opt/data/icebergtable";
    private static final String SOURCE_INSTANCE = "test-instance";
    private static final String SOURCE_DATABASE = "geo";
    private static final String SOURCE_SCHEMA = "public";
    private static final String SOURCE_TABLE = "scalars";
    private static final String RAW_FILE_PATH = "/opt/data/raw/geo/public/scalars/file.jsonl";
    private static final Long RAW_FILE_SIZE = 2048L;
    private static final String EXTRACT_JOB_ID = "abc-123";
    private static final String EXTRACT_BUFFER_ID = "buf-1";
    private static final String EXTRACT_TYPE = "cdc";
    private static final Instant EXTRACT_START_AT = Instant.parse("2026-05-31T02:50:21.544690220Z");
    private static final Instant EXTRACT_END_AT = Instant.parse("2026-05-31T02:51:21.544690220Z");

    @Test
    void serializesWithExpectedJsonPropertyNames() throws Exception {
        TableUpdatedNotification notification = notification();

        JsonNode json = OBJECT_MAPPER.readTree(OBJECT_MAPPER.writeValueAsString(notification));

        assertEquals(WRITE_TYPE, json.get("write_type").asText());
        assertEquals(TABLE_FQN, json.get("table_fqn").asText());
        assertEquals(TABLE_PATH, json.get("table_path").asText());
        assertEquals("ICEBERG", json.get("format").asText());
        assertEquals(CATALOG, json.get("catalog_name").asText());
        assertEquals(DATABASE, json.get("database_name").asText());
        assertEquals(NAMESPACE, json.get("namespace_name").asText());
        assertEquals(TABLE, json.get("table_name").asText());
        assertEquals(WAREHOUSE_PATH, json.get("warehouse_path").asText());
        assertEquals(SOURCE_INSTANCE, json.get("source_instance_name").asText());
        assertEquals(SOURCE_DATABASE, json.get("source_database_name").asText());
        assertEquals(SOURCE_SCHEMA, json.get("source_schema_name").asText());
        assertEquals(SOURCE_TABLE, json.get("source_table_name").asText());
        assertEquals(RAW_FILE_PATH, json.get("raw_file_path").asText());
        assertEquals(RAW_FILE_SIZE, json.get("raw_file_size").asLong());
        assertEquals(EXTRACT_JOB_ID, json.get("extract_job_id").asText());
        assertEquals(EXTRACT_BUFFER_ID, json.get("extract_buffer_id").asText());
        assertEquals(EXTRACT_TYPE, json.get("extract_type").asText());
        assertEquals(EXTRACT_START_AT.toString(), json.get("extract_start_at").asText());
        assertEquals(EXTRACT_END_AT.toString(), json.get("extract_end_at").asText());
        assertEquals(true, json.get("key_schema").isNull());
        assertEquals(true, json.get("value_schema").isNull());
        assertEquals(true, json.get("key_schema_id").isNull());
        assertEquals(true, json.get("value_schema_id").isNull());
    }

    @Test
    void deserializesJsonFormat() throws Exception {
        String json =
                """
                {"write_type":"bronze","table_fqn":"geo.public_bronze.scalars",\
                "table_path":"/opt/data/deltatable/geo/public_bronze/scalars",\
                "format":"DELTA","catalog_name":"lakehouse","database_name":"geo",\
                "namespace_name":"public_bronze","table_name":"scalars",\
                "warehouse_path":"/opt/data/icebergtable",\
                "source_instance_name":"test-instance","source_database_name":"geo",\
                "source_schema_name":"public","source_table_name":"scalars",\
                "raw_file_path":"/opt/data/raw/geo/public/scalars/file.jsonl","raw_file_size":2048,\
                "extract_job_id":"abc-123","extract_buffer_id":"buf-1","extract_type":"cdc",\
                "extract_start_at":"2026-05-31T02:50:21.544690220Z",\
                "extract_end_at":"2026-05-31T02:51:21.544690220Z"}
                """;

        TableUpdatedNotification notification = OBJECT_MAPPER.readValue(json, TableUpdatedNotification.class);

        assertEquals(WRITE_TYPE, notification.writeType());
        assertEquals(TABLE_FQN, notification.tableFqn());
        assertEquals("/opt/data/deltatable/geo/public_bronze/scalars", notification.tablePath());
        assertEquals(OpenTableFormat.DELTA, notification.format());
        assertEquals(CATALOG, notification.catalogName());
        assertEquals(DATABASE, notification.databaseName());
        assertEquals(NAMESPACE, notification.namespaceName());
        assertEquals(TABLE, notification.tableName());
        assertEquals(WAREHOUSE_PATH, notification.warehousePath());
        assertEquals(SOURCE_INSTANCE, notification.sourceInstanceName());
        assertEquals(SOURCE_DATABASE, notification.sourceDatabaseName());
        assertEquals(SOURCE_SCHEMA, notification.sourceSchemaName());
        assertEquals(SOURCE_TABLE, notification.sourceTableName());
        assertEquals(RAW_FILE_PATH, notification.rawFilePath());
        assertEquals(RAW_FILE_SIZE, notification.rawFileSize());
        assertEquals(EXTRACT_JOB_ID, notification.extractJobId());
        assertEquals(EXTRACT_BUFFER_ID, notification.extractBufferId());
        assertEquals(EXTRACT_TYPE, notification.extractType());
        assertEquals(EXTRACT_START_AT, notification.extractStartAt());
        assertEquals(EXTRACT_END_AT, notification.extractEndAt());
    }

    @Test
    void roundTripPreservesAllFields() throws Exception {
        TableUpdatedNotification original = notification();

        String json = OBJECT_MAPPER.writeValueAsString(original);
        TableUpdatedNotification restored = OBJECT_MAPPER.readValue(json, TableUpdatedNotification.class);

        assertEquals(original, restored);
    }

    private static TableUpdatedNotification notification() {
        return new TableUpdatedNotification(
                WRITE_TYPE,
                TABLE_FQN,
                TABLE_PATH,
                FORMAT,
                CATALOG,
                DATABASE,
                NAMESPACE,
                TABLE,
                WAREHOUSE_PATH,
                SOURCE_INSTANCE,
                SOURCE_DATABASE,
                SOURCE_SCHEMA,
                SOURCE_TABLE,
                RAW_FILE_PATH,
                RAW_FILE_SIZE,
                EXTRACT_JOB_ID,
                EXTRACT_BUFFER_ID,
                EXTRACT_TYPE,
                EXTRACT_START_AT,
                EXTRACT_END_AT,
                null,
                null,
                null,
                null);
    }
}
