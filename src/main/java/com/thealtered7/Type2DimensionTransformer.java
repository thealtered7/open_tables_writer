package com.thealtered7;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.lead;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.when;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

public class Type2DimensionTransformer {
    private static final Logger log = LoggerFactory.getLogger(Type2DimensionTransformer.class);
    private static final String SOURCE_UPDATED_AT_COLUMN = "after_updated_at";
    private static final String SILVER_UPDATED_AT_COLUMN = "updated_at";
    private static final String BEFORE_PREFIX = "before_";
    private static final String AFTER_PREFIX = "after_";
    private static final String ID_COLUMN = "id";
    private static final String SOURCE_LSN_COLUMN = "source_lsn";
    private static final String PRIMARY_KEY_COLUMN = "primary_key";
    private static final String VALID_FROM_COLUMN = "valid_from";
    private static final String VALID_TO_COLUMN = "valid_to";
    private static final String IS_CURRENT_COLUMN = "is_current";
    private static final String TYPE2_STAGING_VIEW = "type2_staging";
    private static final String NEXT_UPDATED_AT_COLUMN = "next_updated_at";
    private static final String END_OF_TIME_LITERAL = "10000-12-31 23:59:59.999999";
    private static final Column END_OF_TIME = expr("timestamp '" + END_OF_TIME_LITERAL + "'");
    private static final Set<String> TYPE2_COLUMNS =
            Set.of(VALID_FROM_COLUMN, VALID_TO_COLUMN, IS_CURRENT_COLUMN, PRIMARY_KEY_COLUMN);

    public Type2DimensionTransformer() {
    }

    public void transform(SparkSession spark, TableIdentity table) {
        log.info("transforming table: {}", table.getTableFqn());
        Timestamp maxUpdatedAt = readMaxUpdatedAt(spark, table);
        Dataset<Row> source = readSourceTable(spark, table, maxUpdatedAt);
        if (source.isEmpty()) {
            log.info("no new source rows for {}", table.getTableFqn());
            return;
        }

        Dataset<Row> renamed = transformForType2(source);
        Dataset<Row> type2Rows = buildType2Rows(spark, table, renamed);
        mergeIntoSilver(spark, table, type2Rows);
    }

    private Dataset<Row> buildType2Rows(SparkSession spark, TableIdentity table, Dataset<Row> incoming) {
        String[] baseColumns = baseColumnNames(incoming);
        Dataset<Row> ids = incoming.select(col(ID_COLUMN)).distinct();
        Dataset<Row> combined = incoming.select(toColumns(baseColumns));

        String silverCatalogTable = toSilverCatalogTableName(table);
        if (spark.catalog().tableExists(silverCatalogTable)) {
            Dataset<Row> currentSilver = spark.table(silverCatalogTable)
                    .filter(col(IS_CURRENT_COLUMN).equalTo(true))
                    .join(ids, ID_COLUMN);
            if (!currentSilver.isEmpty()) {
                combined = combined.unionByName(currentSilver.select(toColumns(baseColumns)));
            }
        }

        WindowSpec window = Window.partitionBy(ID_COLUMN).orderBy(col(SOURCE_LSN_COLUMN));
        Column nextUpdatedAt = lead(col(SILVER_UPDATED_AT_COLUMN), 1).over(window);
        Column validFrom = col(SILVER_UPDATED_AT_COLUMN);
        Column validTo = when(
                        col(NEXT_UPDATED_AT_COLUMN).isNotNull(),
                        expr("timestampadd(MICROSECOND, -1, " + NEXT_UPDATED_AT_COLUMN + ")"))
                .otherwise(END_OF_TIME);
        Column isCurrent = validTo.equalTo(END_OF_TIME);
        Column primaryKey = concat(
                col(ID_COLUMN).cast("string"), lit("-"), col(SOURCE_LSN_COLUMN).cast("string"));

        return combined.withColumn(NEXT_UPDATED_AT_COLUMN, nextUpdatedAt)
                .withColumn(VALID_FROM_COLUMN, validFrom)
                .withColumn(VALID_TO_COLUMN, validTo)
                .withColumn(IS_CURRENT_COLUMN, isCurrent)
                .withColumn(PRIMARY_KEY_COLUMN, primaryKey)
                .drop(NEXT_UPDATED_AT_COLUMN);
    }

    private void mergeIntoSilver(SparkSession spark, TableIdentity table, Dataset<Row> type2Rows) {
        String silverCatalogTable = toSilverCatalogTableName(table);
        String sqlTableName = toSqlSilverTableName(table);
        type2Rows.createOrReplaceTempView(TYPE2_STAGING_VIEW);

        if (!spark.catalog().tableExists(silverCatalogTable)) {
            log.info("creating silver type-2 table {}", silverCatalogTable);
            spark.sql(String.format(
                    """
                    CREATE TABLE %s
                    USING iceberg
                    AS SELECT * FROM %s
                    """,
                    sqlTableName,
                    TYPE2_STAGING_VIEW));
            return;
        }

        String updateSetClause = buildMergeUpdateSetClause(type2Rows.columns());
        log.info("merging {} rows into {}", type2Rows.count(), silverCatalogTable);
        spark.sql(String.format(
                """
                MERGE INTO %s AS t
                USING %s AS s
                ON t.%s = s.%s
                WHEN MATCHED THEN UPDATE SET %s
                WHEN NOT MATCHED THEN INSERT *
                """,
                sqlTableName,
                TYPE2_STAGING_VIEW,
                PRIMARY_KEY_COLUMN,
                PRIMARY_KEY_COLUMN,
                updateSetClause));
    }

    private Timestamp readMaxUpdatedAt(SparkSession spark, TableIdentity table) {
        String catalogTable = toSilverCatalogTableName(table);
        if (!spark.catalog().tableExists(catalogTable)) {
            return null;
        }

        Dataset<Row> silver = spark.table(catalogTable);
        if (silver.isEmpty()) {
            return null;
        }

        Row row = silver.agg(max(col(SILVER_UPDATED_AT_COLUMN)).alias(SILVER_UPDATED_AT_COLUMN)).first();
        if (row.isNullAt(0)) {
            return null;
        }
        Timestamp maxUpdatedAt = row.getTimestamp(0);
        log.info("max updated at: {}", maxUpdatedAt);
        return maxUpdatedAt;
    }

    private Dataset<Row> readSourceTable(SparkSession spark, TableIdentity table, Timestamp maxUpdatedAt) {
        Dataset<Row> source = spark.table(table.getCatalogTableName());
        if (maxUpdatedAt == null) {
            return source;
        }
        return source.filter(col(SOURCE_UPDATED_AT_COLUMN).gt(lit(maxUpdatedAt)));
    }

    private Dataset<Row> transformForType2(Dataset<Row> df) {
        List<Column> columns = new ArrayList<>();
        for (String name : df.columns()) {
            if (name.startsWith(BEFORE_PREFIX)) {
                continue;
            }
            if (name.startsWith(AFTER_PREFIX)) {
                columns.add(col(name).alias(name.substring(AFTER_PREFIX.length())));
            } else {
                columns.add(col(name));
            }
        }
        log.info("selecting columns: {}", columns);
        return df.select(columns.toArray(Column[]::new));
    }

    private static String[] baseColumnNames(Dataset<Row> incoming) {
        return Arrays.stream(incoming.columns())
                .filter(name -> !TYPE2_COLUMNS.contains(name))
                .toArray(String[]::new);
    }

    private static Column[] toColumns(String[] columnNames) {
        return Arrays.stream(columnNames).map(org.apache.spark.sql.functions::col).toArray(Column[]::new);
    }

    private static String buildMergeUpdateSetClause(String[] columns) {
        return Arrays.stream(columns)
                .filter(column -> !PRIMARY_KEY_COLUMN.equals(column))
                .map(column -> String.format("t.`%s` = s.`%s`", column, column))
                .collect(Collectors.joining(", "));
    }

    private static String toSilverCatalogTableName(TableIdentity table) {
        return table.getCatalogTableName().replace("local_catalog.", "silver_catalog.");
    }

    private static String toSqlSilverTableName(TableIdentity table) {
        String[] parts = table.getTableFqn().split("\\.");
        return String.format("silver_catalog.`%s`.`%s`.`%s`", parts[0], parts[1], parts[2]);
    }
}
