package com.thealtered7;

import com.thealtered7.models.TableUpdatedNotification;
import java.time.Instant;

/**
 * Postgres source identity and silver Type 1 destination used when registering Type 1 table writes
 * with datapipelines.
 */
public record Type1WriteIdentity(
        String sourceInstanceName,
        String sourceDatabaseName,
        String sourceSchemaName,
        String sourceTableName,
        String databaseName,
        String namespaceName,
        String tableName,
        String rawFilePath,
        Long rawFileSize,
        String extractJobId,
        String extractBufferId,
        String extractType,
        Instant extractStartAt,
        Instant extractEndAt,
        String keySchema,
        String valueSchema,
        String keySchemaId,
        String valueSchemaId,
        Long sourceMinLsn,
        Long sourceMaxLsn) {

    public static String getWriteNamespace(String bronzeNamespace) {
        return OpenTableNamespaces.silverFromBronze(bronzeNamespace);
    }

    public static String getWriteTableName(String sourceTableName) {
        return OpenTableNamespaces.type1Table(sourceTableName);
    }

    public static Type1WriteIdentity fromNotification(TableUpdatedNotification notification) {
        String bronzeNamespace = notification.namespaceName();
        String sourceTableName = firstPresent(notification.sourceTableName(), notification.tableName());
        return new Type1WriteIdentity(
                notification.sourceInstanceName(),
                notification.sourceDatabaseName(),
                notification.sourceSchemaName(),
                notification.sourceTableName(),
                notification.databaseName(),
                getWriteNamespace(bronzeNamespace),
                getWriteTableName(sourceTableName),
                notification.rawFilePath(),
                notification.rawFileSize(),
                notification.extractJobId(),
                notification.extractBufferId(),
                notification.extractType(),
                notification.extractStartAt(),
                notification.extractEndAt(),
                notification.keySchema(),
                notification.valueSchema(),
                notification.keySchemaId(),
                notification.valueSchemaId(),
                notification.sourceMinLsn(),
                notification.sourceMaxLsn());
    }

    public boolean isComplete() {
        return isPresent(sourceInstanceName)
                && isPresent(sourceDatabaseName)
                && isPresent(sourceSchemaName)
                && isPresent(sourceTableName)
                && isPresent(databaseName)
                && isPresent(namespaceName)
                && isPresent(tableName);
    }

    private static String firstPresent(String primary, String fallback) {
        return isPresent(primary) ? primary : fallback;
    }

    private static boolean isPresent(String value) {
        return value != null && !value.isBlank();
    }
}
