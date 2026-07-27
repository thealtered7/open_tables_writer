package com.thealtered7.datapipelines;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/** Request body for the datapipelines {@code POST /bronze-table-writes} endpoint. */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record BronzeTableWriteRequest(
        @JsonProperty("catalog_name") String catalogName,
        @JsonProperty("source_instance_name") String sourceInstanceName,
        @JsonProperty("source_database_name") String sourceDatabaseName,
        @JsonProperty("source_schema_name") String sourceSchemaName,
        @JsonProperty("source_table_name") String sourceTableName,
        @JsonProperty("database_name") String databaseName,
        @JsonProperty("namespace_name") String namespaceName,
        @JsonProperty("table_name") String tableName,
        @JsonProperty("row_count") Long rowCount,
        @JsonProperty("kafka_topic") String kafkaTopic,
        @JsonProperty("kafka_partition") Integer kafkaPartition,
        @JsonProperty("kafka_offset") Long kafkaOffset,
        @JsonProperty("warehouse_path") String warehousePath) {}
