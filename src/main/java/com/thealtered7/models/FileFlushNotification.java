package com.thealtered7.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Instant;

/**
 * Kafka flush notification from pgoutput_to_json. Java fields are camelCase; JSON uses snake_case.
 */
public record FileFlushNotification(
        @JsonProperty("write_type") String writeType,
        @JsonProperty("raw_file_path") String rawFilePath,
        @JsonProperty("table_name") String tableName,
        @JsonProperty("extract_job_id") String extractJobId,
        @JsonProperty("extract_buffer_id") String extractBufferId,
        @JsonProperty("extract_type") String extractType,
        @JsonProperty("extract_start_at") Instant extractStartAt,
        @JsonProperty("extract_end_at") Instant extractEndAt,
        @JsonProperty("write_row_count") Long writeRowCount,
        @JsonProperty("raw_file_size") Long rawFileSize,
        @JsonProperty("source_instance_name") String sourceInstanceName,
        @JsonProperty("source_database_name") String sourceDatabaseName,
        @JsonProperty("source_schema_name") String sourceSchemaName,
        @JsonProperty("source_table_name") String sourceTableName,
        @JsonProperty("warehouse_path") String warehousePath) {}
