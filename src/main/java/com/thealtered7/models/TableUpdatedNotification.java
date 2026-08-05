package com.thealtered7.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Instant;

/**
 * Kafka notification that a bronze open table was written. Java fields are camelCase; JSON uses
 * snake_case aligned with {@code POST /table-writes} source/extract fields plus bronze table
 * identity for downstream silver writers.
 */
public record TableUpdatedNotification(
        @JsonProperty("write_type") String writeType,
        @JsonProperty("table_fqn") String tableFqn,
        @JsonProperty("table_path") String tablePath,
        @JsonProperty("format") OpenTableFormat format,
        @JsonProperty("catalog_name") String catalogName,
        @JsonProperty("database_name") String databaseName,
        @JsonProperty("namespace_name") String namespaceName,
        @JsonProperty("table_name") String tableName,
        @JsonProperty("warehouse_path") String warehousePath,
        @JsonProperty("source_instance_name") String sourceInstanceName,
        @JsonProperty("source_database_name") String sourceDatabaseName,
        @JsonProperty("source_schema_name") String sourceSchemaName,
        @JsonProperty("source_table_name") String sourceTableName,
        @JsonProperty("raw_file_path") String rawFilePath,
        @JsonProperty("raw_file_size") Long rawFileSize,
        @JsonProperty("extract_job_id") String extractJobId,
        @JsonProperty("extract_buffer_id") String extractBufferId,
        @JsonProperty("extract_type") String extractType,
        @JsonProperty("extract_start_at") Instant extractStartAt,
        @JsonProperty("extract_end_at") Instant extractEndAt,
        @JsonProperty("key_schema") String keySchema,
        @JsonProperty("value_schema") String valueSchema,
        @JsonProperty("key_schema_id") String keySchemaId,
        @JsonProperty("value_schema_id") String valueSchemaId,
        @JsonProperty("source_min_lsn") Long sourceMinLsn,
        @JsonProperty("source_max_lsn") Long sourceMaxLsn) {

    public enum OpenTableFormat {
        ICEBERG,
        DELTA;

        public static OpenTableFormat fromString(String value) {
            return OpenTableFormat.valueOf(value.toUpperCase());
        }
    }
}
