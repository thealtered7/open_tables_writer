package com.thealtered7;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

class IcebergType2TableAccess implements Type2TableAccess {

    private final TableIdentity table;

    IcebergType2TableAccess(TableIdentity table) {
        this.table = table;
    }

    @Override
    public String tableFqn() {
        return table.getTableFqn();
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

    private String silverCatalogTableName() {
        return table.getCatalogTableName().replace("local_catalog.", "silver_catalog.");
    }

    private String silverSqlTableName() {
        String[] parts = table.getTableFqn().split("\\.");
        return String.format("silver_catalog.`%s`.`%s`.`%s`", parts[0], parts[1], parts[2]);
    }
}
