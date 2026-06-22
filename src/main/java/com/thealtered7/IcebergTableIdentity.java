package com.thealtered7;

import java.nio.file.Path;

public class IcebergTableIdentity implements TableIdentity {
    private final String instance;
    private final String schema;
    private final String table;
    private final Path warehouse;

    public IcebergTableIdentity(String instance, String schema, String table, Path warehouse) {
        this.instance = instance;
        this.schema = schema;
        this.table = table;
        this.warehouse = warehouse;
    }

    public static IcebergTableIdentity fromTablePath(Path tablePath) {
        Path normalized = tablePath.normalize();
        if (normalized.getNameCount() < 3) {
            throw new IllegalArgumentException(
                    "Table path must contain at least instance/schema/table segments: " + tablePath);
        }

        String table = normalized.getFileName().toString();
        Path schemaPath = normalized.getParent();
        String schema = schemaPath.getFileName().toString();
        Path instancePath = schemaPath.getParent();
        String instance = instancePath.getFileName().toString();
        Path warehouse = instancePath.getParent();
        if (warehouse == null) {
            throw new IllegalArgumentException(
                    "Table path must include a warehouse directory before instance/schema/table: " + tablePath);
        }
        return new IcebergTableIdentity(instance, schema, table, warehouse);
    }

    @Override
    public String getTableFqn() {
        return instance + "." + schema + "." + table;
    }

    @Override
    public String getCatalogTableName() {
        return String.format("local_catalog.%s.%s.%s", instance, schema, table);
    }

    public String getInstance() {
        return instance;
    }

    public String getSchema() {
        return schema;
    }

    public String getTable() {
        return table;
    }

    public Path getWarehouse() {
        return warehouse;
    }
}
