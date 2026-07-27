package com.thealtered7.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Instant;

public record TableUpdatedNotification(
        @JsonProperty("tableFqn") String tableFqn,
        @JsonProperty("tablePath") String tablePath,
        @JsonProperty("format") OpenTableFormat format,
        @JsonProperty("runGuid") String runGuid,
        @JsonProperty("writtenAt") Instant writtenAt,
        @JsonProperty("sourceCatalogName") String sourceCatalogName,
        @JsonProperty("sourceDatabaseName") String sourceDatabaseName,
        @JsonProperty("sourceNamespaceName") String sourceNamespaceName,
        @JsonProperty("sourceTableName") String sourceTableName,
        @JsonProperty("databaseName") String databaseName,
        @JsonProperty("namespaceName") String namespaceName,
        @JsonProperty("tableName") String tableName) {

    public enum OpenTableFormat {
        ICEBERG,
        DELTA;

        public static OpenTableFormat fromString(String value) {
            return OpenTableFormat.valueOf(value.toUpperCase());
        }
    }
}
