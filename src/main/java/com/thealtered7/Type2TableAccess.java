package com.thealtered7;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Isolates the format-specific table I/O (Iceberg catalog tables vs Delta paths) used by
 * {@link Type2DimensionTransformer}. All Type-2 business logic stays in the transformer.
 */
interface Type2TableAccess {

    String tableFqn();

    Dataset<Row> readBronze(SparkSession spark);

    boolean silverExists(SparkSession spark);

    Dataset<Row> readSilver(SparkSession spark);

    void createSilver(SparkSession spark, String stagingView);

    void mergeSilver(SparkSession spark, String stagingView, String onColumn, String updateSetClause);
}
