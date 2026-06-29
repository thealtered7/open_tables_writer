package com.thealtered7;

import com.thealtered7.observability.Observability;
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
import java.util.Map;
import java.util.Objects;
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

    private final Observability observability;

    public Type2DimensionTransformer() {
        this(Observability.noop());
    }

    public Type2DimensionTransformer(Observability observability) {
        this.observability = Objects.requireNonNull(observability, "observability");
    }

    public void transform(SparkSession spark, TableIdentity table) {
        transform(spark, new IcebergType2TableAccess(table));
    }

    public void transform(SparkSession spark, Type2TableAccess access) {
        try {
            observability.observeCallableVoid(
                    Observability.TYPE2_DIMENSION_TRANSFORMER_PREFIX,
                    "transform",
                    Map.of("table", access.tableFqn()),
                    () -> {
                        transformInternal(spark, access);
                        return "success";
                    });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void transformInternal(SparkSession spark, Type2TableAccess access) {
        log.info("transforming table: {}", access.tableFqn());
        Timestamp maxUpdatedAt = readMaxUpdatedAt(spark, access);
        Dataset<Row> source = readSourceTable(spark, access, maxUpdatedAt);
        if (source.isEmpty()) {
            log.info("no new source rows for {}", access.tableFqn());
            return;
        }

        Dataset<Row> renamed = transformForType2(source);
        Dataset<Row> type2Rows = buildType2Rows(spark, access, renamed);
        mergeIntoSilver(spark, access, type2Rows);
    }

    private Dataset<Row> buildType2Rows(SparkSession spark, Type2TableAccess access, Dataset<Row> incoming) {
        String[] baseColumns = baseColumnNames(incoming);
        Dataset<Row> ids = incoming.select(col(ID_COLUMN)).distinct();
        Dataset<Row> combined = incoming.select(toColumns(baseColumns));

        if (access.silverExists(spark)) {
            Dataset<Row> currentSilver = access.readSilver(spark)
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

    private void mergeIntoSilver(SparkSession spark, Type2TableAccess access, Dataset<Row> type2Rows) {
        type2Rows.createOrReplaceTempView(TYPE2_STAGING_VIEW);

        if (!access.silverExists(spark)) {
            log.info("creating silver type-2 table {}", access.tableFqn());
            access.createSilver(spark, TYPE2_STAGING_VIEW);
            return;
        }

        String updateSetClause = buildMergeUpdateSetClause(type2Rows.columns());
        log.info("merging {} rows into {}", type2Rows.count(), access.tableFqn());
        access.mergeSilver(spark, TYPE2_STAGING_VIEW, PRIMARY_KEY_COLUMN, updateSetClause);
    }

    private Timestamp readMaxUpdatedAt(SparkSession spark, Type2TableAccess access) {
        if (!access.silverExists(spark)) {
            return null;
        }

        Dataset<Row> silver = access.readSilver(spark);
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

    private Dataset<Row> readSourceTable(SparkSession spark, Type2TableAccess access, Timestamp maxUpdatedAt) {
        Dataset<Row> source = access.readBronze(spark);
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
}
