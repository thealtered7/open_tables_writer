package com.thealtered7.datapipelines;

/**
 * Format-agnostic description of a completed Type 2 table write. The datapipelines catalog name is
 * supplied by the client, so it is intentionally absent here.
 */
public record Type2TableWriteRegistration(
        String sourceCatalogName,
        String sourceDatabaseName,
        String sourceNamespaceName,
        String sourceTableName,
        String databaseName,
        String namespaceName,
        String tableName,
        Long rowCount,
        String warehousePath,
        KafkaWriteContext kafka) {}
