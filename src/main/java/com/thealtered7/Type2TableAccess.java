package com.thealtered7;

import com.thealtered7.models.TableUpdatedNotification.OpenTableFormat;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Isolates the format-specific table I/O (Iceberg catalog tables vs Delta paths) used by
 * {@link Type2DimensionTransformer}. All Type-2 business logic stays in the transformer.
 */
interface Type2TableAccess {

    String tableFqn();

    OpenTableFormat format();

    /** Absolute path of the warehouse where silver Type-2 tables for this access are written. */
    String warehousePath();

    Dataset<Row> readBronze(SparkSession spark);

    boolean silverExists(SparkSession spark);

    Dataset<Row> readSilver(SparkSession spark);

    void createSilver(SparkSession spark, String stagingView);

    void mergeSilver(SparkSession spark, String stagingView, String onColumn, String updateSetClause);

    boolean type1Exists(SparkSession spark);

    void createType1(SparkSession spark, String stagingView);

    void mergeType1(SparkSession spark, String stagingView, String onColumn, String updateSetClause);

    void addIsDeletedColumn(SparkSession spark, String columnName);

    void addVersionKeyColumn(SparkSession spark, String columnName, String primaryKeyColumn, String validFromColumn);

    /** Spark catalog name used with {@code spark.table(...)} / {@code tableExists}. */
    String silverCatalogTableName();

    /** Backtick-quoted Spark SQL name for ALTER/MERGE DDL. */
    String silverSqlTableName();

    String type1CatalogTableName();

    String type1SqlTableName();
}
