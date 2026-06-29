package com.thealtered7.models;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.thealtered7.models.TableUpdatedNotification.OpenTableFormat;
import java.time.Instant;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TableUpdatedNotificationTest {

    private static final ObjectMapper OBJECT_MAPPER = TableUpdatedNotificationJson.MAPPER;

    private static final String TABLE_FQN = "geo.public.scalars";
    private static final String TABLE_PATH = "/opt/data/iceberg/geo/public/scalars";
    private static final OpenTableFormat FORMAT = OpenTableFormat.ICEBERG;
    private static final String RUN_GUID = "abc-123";
    private static final Instant WRITTEN_AT = Instant.parse("2026-05-31T02:51:21.544690220Z");

    @Test
    void serializesWithExpectedJsonPropertyNames() throws Exception {
        TableUpdatedNotification notification =
                new TableUpdatedNotification(TABLE_FQN, TABLE_PATH, FORMAT, RUN_GUID, WRITTEN_AT);

        JsonNode json = OBJECT_MAPPER.readTree(OBJECT_MAPPER.writeValueAsString(notification));

        assertEquals(TABLE_FQN, json.get("tableFqn").asText());
        assertEquals(TABLE_PATH, json.get("tablePath").asText());
        assertEquals("ICEBERG", json.get("format").asText());
        assertEquals(RUN_GUID, json.get("runGuid").asText());
        assertEquals(WRITTEN_AT.toString(), json.get("writtenAt").asText());
    }

    @Test
    void deserializesJsonFormat() throws Exception {
        String json =
                """
                {"tableFqn":"geo.public.scalars",\
                "tablePath":"/opt/data/deltatable/geo/public/scalars",\
                "format":"DELTA","runGuid":"abc-123",\
                "writtenAt":"2026-05-31T02:51:21.544690220Z"}
                """;

        TableUpdatedNotification notification = OBJECT_MAPPER.readValue(json, TableUpdatedNotification.class);

        assertEquals(TABLE_FQN, notification.tableFqn());
        assertEquals("/opt/data/deltatable/geo/public/scalars", notification.tablePath());
        assertEquals(OpenTableFormat.DELTA, notification.format());
        assertEquals(RUN_GUID, notification.runGuid());
        assertEquals(WRITTEN_AT, notification.writtenAt());
    }

    @Test
    void roundTripPreservesAllFields() throws Exception {
        TableUpdatedNotification original =
                new TableUpdatedNotification(TABLE_FQN, TABLE_PATH, FORMAT, RUN_GUID, WRITTEN_AT);

        String json = OBJECT_MAPPER.writeValueAsString(original);
        TableUpdatedNotification restored = OBJECT_MAPPER.readValue(json, TableUpdatedNotification.class);

        assertEquals(original, restored);
    }
}
