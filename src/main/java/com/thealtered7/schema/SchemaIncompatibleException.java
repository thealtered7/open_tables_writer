package com.thealtered7.schema;

/**
 * Raised when an Iceberg table column type cannot be safely coerced to match an incoming schema.
 */
public class SchemaIncompatibleException extends RuntimeException {

    private final String tableName;
    private final String columnName;
    private final String fromType;
    private final String toType;

    public SchemaIncompatibleException(String tableName, String columnName, String fromType, String toType) {
        super(String.format(
                "Unsupported schema type change on %s.%s: %s -> %s; human intervention required",
                tableName, columnName, fromType, toType));
        this.tableName = tableName;
        this.columnName = columnName;
        this.fromType = fromType;
        this.toType = toType;
    }

    public String tableName() {
        return tableName;
    }

    public String columnName() {
        return columnName;
    }

    public String fromType() {
        return fromType;
    }

    public String toType() {
        return toType;
    }
}
