package com.thealtered7;

import com.thealtered7.models.TableUpdatedNotification.OpenTableFormat;
import java.nio.file.Path;
import java.util.Objects;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

class IcebergType2TableAccess implements Type2TableAccess {

    private final TableIdentity table;
    private final Path silverWarehouse;

    IcebergType2TableAccess(TableIdentity table, Path silverWarehouse) {
        this.table = Objects.requireNonNull(table, "table");
        this.silverWarehouse = Objects.requireNonNull(silverWarehouse, "silverWarehouse");
    }

    @Override
    public String tableFqn() {
        return table.getTableFqn();
    }

    @Override
    public OpenTableFormat format() {
        return OpenTableFormat.ICEBERG;
    }

    @Override
    public String warehousePath() {
        return silverWarehouse.toAbsolutePath().toString();
    }

    @Override
    public Dataset<Row> readBronze(SparkSession spark) {
        return spark.table(table.getCatalogTableName());
    }

    @Override
    public boolean silverExists(SparkSession spark) {
        return spark.catalog().tableExists(silverCatalogTableName());
    }

    @Override
    public Dataset<Row> readSilver(SparkSession spark) {
        return spark.table(silverCatalogTableName());
    }

    @Override
    public void createSilver(SparkSession spark, String stagingView) {
        spark.sql(String.format(
                """
                CREATE TABLE %s
                USING iceberg
                AS SELECT * FROM %s
                """,
                silverSqlTableName(),
                stagingView));
    }

    @Override
    public void mergeSilver(SparkSession spark, String stagingView, String onColumn, String updateSetClause) {
        spark.sql(String.format(
                """
                MERGE INTO %s AS t
                USING %s AS s
                ON t.%s = s.%s
                WHEN MATCHED THEN UPDATE SET %s
                WHEN NOT MATCHED THEN INSERT *
                """,
                silverSqlTableName(),
                stagingView,
                onColumn,
                onColumn,
                updateSetClause));
    }

    @Override
    public void addIsDeletedColumn(SparkSession spark, String columnName) {
        spark.sql(String.format("ALTER TABLE %s ADD COLUMN %s boolean", silverSqlTableName(), columnName));
        spark.sql(String.format(
                "UPDATE %s SET %s = false WHERE %s IS NULL", silverSqlTableName(), columnName, columnName));
    }

    private String silverCatalogTableName() {
        String[] parts = table.getTableFqn().split("\\.");
        String silverNamespace = OpenTableNamespaces.silverFromBronze(parts[1]);
        String silverTable = OpenTableNamespaces.type2Table(parts[2]);
        return String.format("silver_catalog.%s.%s.%s", parts[0], silverNamespace, silverTable);
    }

    private String silverSqlTableName() {
        String[] parts = table.getTableFqn().split("\\.");
        String silverNamespace = OpenTableNamespaces.silverFromBronze(parts[1]);
        String silverTable = OpenTableNamespaces.type2Table(parts[2]);
        return String.format("silver_catalog.`%s`.`%s`.`%s`", parts[0], silverNamespace, silverTable);
    }
}
