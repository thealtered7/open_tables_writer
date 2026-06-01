package com.thealtered7;

import java.nio.file.Path;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import io.delta.tables.DeltaTable;

public class DeltaTableOperations {

    public DeltaTableOperations() {
    }

    public boolean tableExists(SparkSession spark, Path outputTablePath) {
        return DeltaTable.isDeltaTable(spark, outputTablePath.toString());
    }

    public void createPartitionedTable(Dataset<Row> partitioned, Path outputTablePath) {
        partitioned.write()
                .format("delta")
                .partitionBy("year", "month", "day")
                .save(outputTablePath.toString());
    }

    public void appendToTable(Dataset<Row> partitioned, Path outputTablePath) {
        partitioned.write()
                .format("delta")
                .mode("append")
                .save(outputTablePath.toString());
    }
}
