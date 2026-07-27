package com.thealtered7;

import com.thealtered7.models.FileFlushNotification;

/**
 * OLTP source table identity carried on {@code cdc-file-write} flush notifications and used when
 * registering bronze table writes with datapipelines.
 */
public record SourceTableIdentity(
        String instanceName, String databaseName, String schemaName, String tableName) {

    public static SourceTableIdentity fromFlush(FileFlushNotification notification) {
        return new SourceTableIdentity(
                notification.sourceInstanceName(),
                notification.sourceDatabaseName(),
                notification.sourceSchemaName(),
                notification.sourceTableName());
    }

    public boolean isComplete() {
        return isPresent(instanceName)
                && isPresent(databaseName)
                && isPresent(schemaName)
                && isPresent(tableName);
    }

    private static boolean isPresent(String value) {
        return value != null && !value.isBlank();
    }
}
