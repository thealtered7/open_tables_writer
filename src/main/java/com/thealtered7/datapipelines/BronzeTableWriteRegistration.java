package com.thealtered7.datapipelines;

/**
 * Format-agnostic description of a completed bronze table write. The datapipelines catalog name is
 * supplied by the client, so it is intentionally absent here.
 */
public record BronzeTableWriteRegistration(
        String sourceInstanceName,
        String sourceDatabaseName,
        String sourceSchemaName,
        String sourceTableName,
        String databaseName,
        String namespaceName,
        String tableName,
        Long rowCount,
        String warehousePath,
        KafkaWriteContext kafka) {}
