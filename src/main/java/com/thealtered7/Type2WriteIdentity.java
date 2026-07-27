package com.thealtered7;

import com.thealtered7.models.TableUpdatedNotification;

/**
 * Source (bronze open table) and destination (silver Type 2) identity used when registering Type 2
 * table writes with datapipelines.
 */
public record Type2WriteIdentity(
        String sourceCatalogName,
        String sourceDatabaseName,
        String sourceNamespaceName,
        String sourceTableName,
        String databaseName,
        String namespaceName,
        String tableName) {

    public static String getWriteNamespace(String sourceNamespaceName) {
        return OpenTableNamespaces.silverFromBronze(sourceNamespaceName);
    }

    public static String getWriteTableName(String sourceTableName) {
        return OpenTableNamespaces.type2Table(sourceTableName);
    }

    public static Type2WriteIdentity fromNotification(TableUpdatedNotification notification) {
        String bronzeNamespace = notification.namespaceName() != null && !notification.namespaceName().isBlank()
                ? notification.namespaceName()
                : notification.sourceNamespaceName();
        String sourceTableName = notification.sourceTableName() != null && !notification.sourceTableName().isBlank()
                ? notification.sourceTableName()
                : notification.tableName();
        return new Type2WriteIdentity(
                notification.sourceCatalogName(),
                notification.sourceDatabaseName(),
                bronzeNamespace,
                sourceTableName,
                notification.databaseName(),
                getWriteNamespace(bronzeNamespace),
                getWriteTableName(sourceTableName));
    }

    public boolean isComplete() {
        return isPresent(sourceCatalogName)
                && isPresent(sourceDatabaseName)
                && isPresent(sourceNamespaceName)
                && isPresent(sourceTableName)
                && isPresent(databaseName)
                && isPresent(namespaceName)
                && isPresent(tableName);
    }

    private static boolean isPresent(String value) {
        return value != null && !value.isBlank();
    }
}
