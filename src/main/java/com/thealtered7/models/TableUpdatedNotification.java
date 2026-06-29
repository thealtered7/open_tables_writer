package com.thealtered7.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Instant;

public record TableUpdatedNotification(
        @JsonProperty("tableFqn") String tableFqn,
        @JsonProperty("tablePath") String tablePath,
        @JsonProperty("format") OpenTableFormat format,
        @JsonProperty("runGuid") String runGuid,
        @JsonProperty("writtenAt") Instant writtenAt) {

    public enum OpenTableFormat {
        ICEBERG,
        DELTA;

        public static OpenTableFormat fromString(String value) {
            return OpenTableFormat.valueOf(value.toUpperCase());
        }
    }
}
