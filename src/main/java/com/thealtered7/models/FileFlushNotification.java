package com.thealtered7.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Instant;

public record FileFlushNotification(
        @JsonProperty("filePath") String filePath,
        @JsonProperty("tableName") String tableName,
        @JsonProperty("runGuid") String runGuid,
        @JsonProperty("writtenAt") Instant writtenAt) {}
