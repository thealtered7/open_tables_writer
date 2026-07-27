package com.thealtered7.models;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FileFlushNotificationTest {

    private static final ObjectMapper OBJECT_MAPPER = FileFlushNotificationJson.MAPPER;

    private static final String FILE_PATH =
            "/opt/data/raw/geo.public.scalars-2026-05-31_02-51-21.jsonl";
    private static final String TABLE_NAME = "geo.public.scalars";
    private static final String RUN_GUID = "abc-123";
    private static final Instant WRITTEN_AT = Instant.parse("2026-05-31T02:51:21.544690220Z");
    private static final String SOURCE_INSTANCE = "test-instance";
    private static final String SOURCE_DATABASE = "geo";
    private static final String SOURCE_SCHEMA = "public";
    private static final String SOURCE_TABLE = "scalars";

    @Test
    void serializesWithExpectedJsonPropertyNames() throws Exception {
        FileFlushNotification notification = notification();

        JsonNode json = OBJECT_MAPPER.readTree(OBJECT_MAPPER.writeValueAsString(notification));

        assertEquals(FILE_PATH, json.get("filePath").asText());
        assertEquals(TABLE_NAME, json.get("tableName").asText());
        assertEquals(RUN_GUID, json.get("runGuid").asText());
        assertEquals(WRITTEN_AT.toString(), json.get("writtenAt").asText());
        assertEquals(SOURCE_INSTANCE, json.get("sourceInstanceName").asText());
        assertEquals(SOURCE_DATABASE, json.get("sourceDatabaseName").asText());
        assertEquals(SOURCE_SCHEMA, json.get("sourceSchemaName").asText());
        assertEquals(SOURCE_TABLE, json.get("sourceTableName").asText());
    }

    @Test
    void deserializesPgoutputJsonFormat() throws Exception {
        String json =
                """
                {"filePath":"/opt/data/raw/geo.public.scalars-2026-05-31_02-51-21.jsonl",\
                "tableName":"geo.public.scalars","runGuid":"abc-123",\
                "writtenAt":"2026-05-31T02:51:21.544690220Z",\
                "sourceInstanceName":"test-instance","sourceDatabaseName":"geo",\
                "sourceSchemaName":"public","sourceTableName":"scalars"}
                """;

        FileFlushNotification notification = OBJECT_MAPPER.readValue(json, FileFlushNotification.class);

        assertEquals(FILE_PATH, notification.filePath());
        assertEquals(TABLE_NAME, notification.tableName());
        assertEquals(RUN_GUID, notification.runGuid());
        assertEquals(WRITTEN_AT, notification.writtenAt());
        assertEquals(SOURCE_INSTANCE, notification.sourceInstanceName());
        assertEquals(SOURCE_DATABASE, notification.sourceDatabaseName());
        assertEquals(SOURCE_SCHEMA, notification.sourceSchemaName());
        assertEquals(SOURCE_TABLE, notification.sourceTableName());
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
                FILE_PATH,
                TABLE_NAME,
                RUN_GUID,
                WRITTEN_AT,
                SOURCE_INSTANCE,
                SOURCE_DATABASE,
                SOURCE_SCHEMA,
                SOURCE_TABLE);
    }
}
