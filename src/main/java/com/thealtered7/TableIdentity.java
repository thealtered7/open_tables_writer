package com.thealtered7;

import java.nio.file.Path;

public interface TableIdentity {
    public String getTableFqn();
    public String getCatalogTableName();
    public Path getWarehouse();
}
