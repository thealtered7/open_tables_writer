package com.thealtered7.schema;

import org.apache.spark.sql.types.DataType;

/**
 * A business column declared in a Debezium Connect schema (typically under {@code after}/{@code before}).
 */
public record ConnectField(String name, DataType dataType, String sqlType, boolean optional, String defaultLiteral) {

    public boolean hasDefault() {
        return defaultLiteral != null;
    }
}
