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

    @Test
    void serializesWithExpectedJsonPropertyNames() throws Exception {
        FileFlushNotification notification =
                new FileFlushNotification(FILE_PATH, TABLE_NAME, RUN_GUID, WRITTEN_AT);

        JsonNode json = OBJECT_MAPPER.readTree(OBJECT_MAPPER.writeValueAsString(notification));

        assertEquals(FILE_PATH, json.get("filePath").asText());
        assertEquals(TABLE_NAME, json.get("tableName").asText());
        assertEquals(RUN_GUID, json.get("runGuid").asText());
        assertEquals(WRITTEN_AT.toString(), json.get("writtenAt").asText());
    }

    @Test
    void deserializesPgoutputJsonFormat() throws Exception {
        String json =
                """
                {"filePath":"/opt/data/raw/geo.public.scalars-2026-05-31_02-51-21.jsonl",\
                "tableName":"geo.public.scalars","runGuid":"abc-123",\
                "writtenAt":"2026-05-31T02:51:21.544690220Z"}
                """;

        FileFlushNotification notification = OBJECT_MAPPER.readValue(json, FileFlushNotification.class);

        assertEquals(FILE_PATH, notification.filePath());
        assertEquals(TABLE_NAME, notification.tableName());
        assertEquals(RUN_GUID, notification.runGuid());
        assertEquals(WRITTEN_AT, notification.writtenAt());
    }

    @Test
    void roundTripPreservesAllFields() throws Exception {
        FileFlushNotification original =
                new FileFlushNotification(FILE_PATH, TABLE_NAME, RUN_GUID, WRITTEN_AT);

        String json = OBJECT_MAPPER.writeValueAsString(original);
        FileFlushNotification restored = OBJECT_MAPPER.readValue(json, FileFlushNotification.class);

        assertEquals(original, restored);
    }
}
