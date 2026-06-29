package com.thealtered7;

import io.delta.tables.DeltaTable;
import java.nio.file.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

class DeltaType2TableAccess implements Type2TableAccess {

    private final DeltaTableIdentity table;
    private final Path silverTablePath;

    DeltaType2TableAccess(DeltaTableIdentity table, Path silverWarehouse) {
        this.table = table;
        this.silverTablePath = table.getSilverTablePath(silverWarehouse);
    }

    @Override
    public String tableFqn() {
        return table.getTableFqn();
    }

    @Override
    public Dataset<Row> readBronze(SparkSession spark) {
        return spark.read().format("delta").load(table.getBronzeTablePath().toString());
    }

    @Override
    public boolean silverExists(SparkSession spark) {
        return DeltaTable.isDeltaTable(spark, silverTablePath.toString());
    }

    @Override
    public Dataset<Row> readSilver(SparkSession spark) {
        return spark.read().format("delta").load(silverTablePath.toString());
    }

    @Override
    public void createSilver(SparkSession spark, String stagingView) {
        spark.table(stagingView).write().format("delta").save(silverTablePath.toString());
    }

    @Override
    public void mergeSilver(SparkSession spark, String stagingView, String onColumn, String updateSetClause) {
        spark.sql(String.format(
                """
                MERGE INTO delta.`%s` AS t
                USING %s AS s
                ON t.%s = s.%s
                WHEN MATCHED THEN UPDATE SET %s
                WHEN NOT MATCHED THEN INSERT *
                """,
                silverTablePath,
                stagingView,
                onColumn,
                onColumn,
                updateSetClause));
    }
}
