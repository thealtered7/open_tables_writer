package com.thealtered7.models;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.thealtered7.models.TableUpdatedNotification.OpenTableFormat;
import java.time.Instant;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TableUpdatedNotificationTest {

    private static final ObjectMapper OBJECT_MAPPER = TableUpdatedNotificationJson.MAPPER;

    private static final String TABLE_FQN = "geo.public_bronze.scalars";
    private static final String TABLE_PATH = "/opt/data/iceberg/geo/public_bronze/scalars";
    private static final OpenTableFormat FORMAT = OpenTableFormat.ICEBERG;
    private static final String RUN_GUID = "abc-123";
    private static final Instant WRITTEN_AT = Instant.parse("2026-05-31T02:51:21.544690220Z");
    private static final String SOURCE_CATALOG = "lakehouse";
    private static final String SOURCE_DATABASE = "geo";
    private static final String SOURCE_NAMESPACE = "public_bronze";
    private static final String SOURCE_TABLE = "scalars";
    private static final String DATABASE = "geo";
    private static final String NAMESPACE = "public_bronze";
    private static final String TABLE = "scalars";

    @Test
    void serializesWithExpectedJsonPropertyNames() throws Exception {
        TableUpdatedNotification notification = notification();

        JsonNode json = OBJECT_MAPPER.readTree(OBJECT_MAPPER.writeValueAsString(notification));

        assertEquals(TABLE_FQN, json.get("tableFqn").asText());
        assertEquals(TABLE_PATH, json.get("tablePath").asText());
        assertEquals("ICEBERG", json.get("format").asText());
        assertEquals(RUN_GUID, json.get("runGuid").asText());
        assertEquals(WRITTEN_AT.toString(), json.get("writtenAt").asText());
        assertEquals(SOURCE_CATALOG, json.get("sourceCatalogName").asText());
        assertEquals(SOURCE_DATABASE, json.get("sourceDatabaseName").asText());
        assertEquals(SOURCE_NAMESPACE, json.get("sourceNamespaceName").asText());
        assertEquals(SOURCE_TABLE, json.get("sourceTableName").asText());
        assertEquals(DATABASE, json.get("databaseName").asText());
        assertEquals(NAMESPACE, json.get("namespaceName").asText());
        assertEquals(TABLE, json.get("tableName").asText());
    }

    @Test
    void deserializesJsonFormat() throws Exception {
        String json =
                """
                {"tableFqn":"geo.public_bronze.scalars",\
                "tablePath":"/opt/data/deltatable/geo/public_bronze/scalars",\
                "format":"DELTA","runGuid":"abc-123",\
                "writtenAt":"2026-05-31T02:51:21.544690220Z",\
                "sourceCatalogName":"lakehouse","sourceDatabaseName":"geo",\
                "sourceNamespaceName":"public_bronze","sourceTableName":"scalars",\
                "databaseName":"geo","namespaceName":"public_bronze","tableName":"scalars"}
                """;

        TableUpdatedNotification notification = OBJECT_MAPPER.readValue(json, TableUpdatedNotification.class);

        assertEquals(TABLE_FQN, notification.tableFqn());
        assertEquals("/opt/data/deltatable/geo/public_bronze/scalars", notification.tablePath());
        assertEquals(OpenTableFormat.DELTA, notification.format());
        assertEquals(RUN_GUID, notification.runGuid());
        assertEquals(WRITTEN_AT, notification.writtenAt());
        assertEquals(SOURCE_CATALOG, notification.sourceCatalogName());
        assertEquals(SOURCE_DATABASE, notification.sourceDatabaseName());
        assertEquals(SOURCE_NAMESPACE, notification.sourceNamespaceName());
        assertEquals(SOURCE_TABLE, notification.sourceTableName());
        assertEquals(DATABASE, notification.databaseName());
        assertEquals(NAMESPACE, notification.namespaceName());
        assertEquals(TABLE, notification.tableName());
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
                TABLE_FQN,
                TABLE_PATH,
                FORMAT,
                RUN_GUID,
                WRITTEN_AT,
                SOURCE_CATALOG,
                SOURCE_DATABASE,
                SOURCE_NAMESPACE,
                SOURCE_TABLE,
                DATABASE,
                NAMESPACE,
                TABLE);
    }
}
