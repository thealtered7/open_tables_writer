package com.thealtered7.datapipelines;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Instant;

/**
 * Request body for the datapipelines {@code POST /table-writes} endpoint. Field names use the
 * snake_case shape the service expects.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record TableWriteRequest(
        @JsonProperty("write_type") String writeType,
        @JsonProperty("catalog_name") String catalogName,
        @JsonProperty("namespace_name") String namespaceName,
        @JsonProperty("database_name") String databaseName,
        @JsonProperty("table_name") String tableName,
        @JsonProperty("source_instance_name") String sourceInstanceName,
        @JsonProperty("source_database_name") String sourceDatabaseName,
        @JsonProperty("source_schema_name") String sourceSchemaName,
        @JsonProperty("source_table_name") String sourceTableName,
        @JsonProperty("kafka_topic") String kafkaTopic,
        @JsonProperty("kafka_partition") Integer kafkaPartition,
        @JsonProperty("kafka_offset") Long kafkaOffset,
        @JsonProperty("write_row_count") Long writeRowCount,
        @JsonProperty("merge_row_count") Long mergeRowCount,
        @JsonProperty("raw_file_path") String rawFilePath,
        @JsonProperty("raw_file_size") Long rawFileSize,
        @JsonProperty("extract_job_id") String extractJobId,
        @JsonProperty("extract_buffer_id") String extractBufferId,
        @JsonProperty("extract_type") String extractType,
        @JsonProperty("extract_start_at") Instant extractStartAt,
        @JsonProperty("extract_end_at") Instant extractEndAt,
        @JsonProperty("merge_start_at") Instant mergeStartAt,
        @JsonProperty("merge_end_at") Instant mergeEndAt,
        @JsonProperty("warehouse_path") String warehousePath,
        @JsonProperty("key_schema") String keySchema,
        @JsonProperty("value_schema") String valueSchema,
        @JsonProperty("key_schema_id") String keySchemaId,
        @JsonProperty("value_schema_id") String valueSchemaId,
        @JsonProperty("source_min_lsn") Long sourceMinLsn,
        @JsonProperty("source_max_lsn") Long sourceMaxLsn) {}
