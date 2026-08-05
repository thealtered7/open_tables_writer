package com.thealtered7.datapipelines;

import java.time.Instant;

/**
 * Format-agnostic description of a completed table write. The datapipelines catalog name is
 * supplied by the client, so it is intentionally absent here.
 */
public record TableWriteRegistration(
        String writeType,
        String databaseName,
        String namespaceName,
        String tableName,
        String sourceInstanceName,
        String sourceDatabaseName,
        String sourceSchemaName,
        String sourceTableName,
        Long writeRowCount,
        Long mergeRowCount,
        String rawFilePath,
        Long rawFileSize,
        String extractJobId,
        String extractBufferId,
        String extractType,
        Instant extractStartAt,
        Instant extractEndAt,
        Instant mergeStartAt,
        Instant mergeEndAt,
        String warehousePath,
        KafkaWriteContext kafka,
        String keySchema,
        String valueSchema,
        String keySchemaId,
        String valueSchemaId,
        Long sourceMinLsn,
        Long sourceMaxLsn) {

    public static final String WRITE_TYPE_BRONZE = "bronze";
    public static final String WRITE_TYPE_SILVER_TYPE_1 = "silver_type_1";
    public static final String WRITE_TYPE_SILVER_TYPE_2 = "silver_type_2";
}
