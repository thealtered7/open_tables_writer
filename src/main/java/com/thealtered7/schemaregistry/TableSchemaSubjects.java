package com.thealtered7.schemaregistry;

/** Builds Confluent/Glue subject names for lakehouse Iceberg table schemas. */
public final class TableSchemaSubjects {

    private TableSchemaSubjects() {}

    /**
     * Value subject for a lakehouse table: {@code iceberg.{database}.{namespace}.{table}-value}.
     */
    public static String valueSubject(String databaseName, String namespaceName, String tableName) {
        return "iceberg." + databaseName + "." + namespaceName + "." + tableName + "-value";
    }
}
