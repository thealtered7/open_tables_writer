package com.thealtered7;

import java.nio.file.Path;

public class DeltaTableIdentity {
    private final String instance;
    private final String schema;
    private final String table;
    private final Path bronzeTablePath;

    public DeltaTableIdentity(String instance, String schema, String table, Path bronzeTablePath) {
        this.instance = instance;
        this.schema = schema;
        this.table = table;
        this.bronzeTablePath = bronzeTablePath;
    }

    public static DeltaTableIdentity fromTablePath(Path tablePath) {
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
        return new DeltaTableIdentity(instance, schema, table, normalized);
    }

    public String getTableFqn() {
        return instance + "." + schema + "." + table;
    }

    public Path getBronzeTablePath() {
        return bronzeTablePath;
    }

    public Path getSilverTablePath(Path silverWarehouse) {
        return silverWarehouse
                .resolve("delta")
                .resolve(instance)
                .resolve(schema)
                .resolve(table);
    }
}
