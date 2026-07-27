package com.thealtered7;

import com.thealtered7.datapipelines.DatapipelinesClient;
import com.thealtered7.datapipelines.KafkaWriteContext;
import com.thealtered7.datapipelines.NoopDatapipelinesClient;
import com.thealtered7.datapipelines.Type2TableWriteRegistration;
import com.thealtered7.models.TableUpdatedNotification.OpenTableFormat;
import com.thealtered7.observability.Observability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.lead;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.row_number;
import static org.apache.spark.sql.functions.timestamp_millis;
import static org.apache.spark.sql.functions.to_utc_timestamp;
import static org.apache.spark.sql.functions.when;

import java.sql.Timestamp;
import java.nio.file.Path;
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
    private static final String IS_DELETED_COLUMN = "is_deleted";
    private static final String OP_COLUMN = "op";
    private static final String TS_MS_COLUMN = "ts_ms";
    private static final String DELETE_OP = "d";
    private static final String DELETION_TIME_COLUMN = "deletion_time";
    private static final String ROW_NUMBER_COLUMN = "row_number";
    private static final String TYPE2_STAGING_VIEW_PREFIX = "type2_staging";
    private static final String NEXT_UPDATED_AT_COLUMN = "next_updated_at";
    private static final String END_OF_TIME_LITERAL = "10000-12-31 23:59:59.999999";
    private static final String BEGIN_OF_TIME_LITERAL = "1970-01-01 00:00:00";
    private static final Column END_OF_TIME = expr("timestamp '" + END_OF_TIME_LITERAL + "'");
    private static final Column BEGIN_OF_TIME = expr("timestamp '" + BEGIN_OF_TIME_LITERAL + "'");
    private static final Set<String> TYPE2_COLUMNS = Set.of(
            VALID_FROM_COLUMN, VALID_TO_COLUMN, IS_CURRENT_COLUMN, PRIMARY_KEY_COLUMN, IS_DELETED_COLUMN);

    private final Observability observability;
    private final DatapipelinesClient datapipelinesClient;

    public Type2DimensionTransformer() {
        this(Observability.noop());
    }

    public Type2DimensionTransformer(Observability observability) {
        this(observability, new NoopDatapipelinesClient());
    }

    public Type2DimensionTransformer(Observability observability, DatapipelinesClient datapipelinesClient) {
        this.observability = Objects.requireNonNull(observability, "observability");
        this.datapipelinesClient = Objects.requireNonNull(datapipelinesClient, "datapipelinesClient");
    }

    public void transform(SparkSession spark, TableIdentity table) {
        Path silverWarehouse = Path.of(spark.conf().get("spark.sql.catalog.silver_catalog.warehouse"));
        transform(spark, new IcebergType2TableAccess(table, silverWarehouse));
    }

    public void transform(SparkSession spark, Type2TableAccess access) {
        transform(spark, access, null, null);
    }

    public void transform(SparkSession spark, Type2TableAccess access, KafkaWriteContext kafka) {
        transform(spark, access, kafka, null);
    }

    public void transform(
            SparkSession spark, Type2TableAccess access, KafkaWriteContext kafka, Type2WriteIdentity writeIdentity) {
        try {
            observability.observeCallableVoid(
                    Observability.TYPE2_DIMENSION_TRANSFORMER_PREFIX,
                    "transform",
                    Map.of("table", access.tableFqn()),
                    () -> {
                        transformInternal(spark, access, kafka, writeIdentity);
                        return "success";
                    });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void transformInternal(
            SparkSession spark, Type2TableAccess access, KafkaWriteContext kafka, Type2WriteIdentity writeIdentity) {
        log.info("transforming table: {}", access.tableFqn());
        Timestamp maxUpdatedAt = readMaxUpdatedAt(spark, access);
        Dataset<Row> source = readSourceTable(spark, access, maxUpdatedAt);
        if (source.isEmpty()) {
            log.info("no new source rows for {}", access.tableFqn());
            return;
        }

        Dataset<Row> deletes = source.filter(col(OP_COLUMN).equalTo(DELETE_OP));
        Dataset<Row> upserts = source.filter(col(OP_COLUMN).notEqual(DELETE_OP).or(col(OP_COLUMN).isNull()));

        Dataset<Row> result = null;
        if (!upserts.isEmpty()) {
            result = buildType2Rows(spark, access, transformForType2(upserts));
        }
        if (!deletes.isEmpty()) {
            Dataset<Row> deleteRows = buildDeleteType2Rows(spark, access, deletes);
            result = result == null ? deleteRows : result.unionByName(deleteRows, true);
        }
        if (result == null) {
            log.info("no upsert or delete rows to apply for {}", access.tableFqn());
            return;
        }
        mergeIntoSilver(spark, access, result, kafka, writeIdentity);
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
                .withColumn(IS_DELETED_COLUMN, lit(false))
                .drop(NEXT_UPDATED_AT_COLUMN);
    }

    private Dataset<Row> buildDeleteType2Rows(SparkSession spark, Type2TableAccess access, Dataset<Row> deletesRaw) {
        Dataset<Row> deletes = transformDeletesForType2(deletesRaw)
                .withColumn(DELETION_TIME_COLUMN, deletionTime(spark));

        WindowSpec latestDelete = Window.partitionBy(ID_COLUMN).orderBy(col(SOURCE_LSN_COLUMN).desc());
        Dataset<Row> dedupedDeletes = deletes
                .withColumn(ROW_NUMBER_COLUMN, row_number().over(latestDelete))
                .filter(col(ROW_NUMBER_COLUMN).equalTo(1))
                .drop(ROW_NUMBER_COLUMN);

        Dataset<Row> tombstoneDeletes = dedupedDeletes;
        Dataset<Row> closedRows = null;

        if (access.silverExists(spark)) {
            Dataset<Row> incomingIds = dedupedDeletes.select(col(ID_COLUMN)).distinct();
            WindowSpec recency = Window.partitionBy(ID_COLUMN)
                    .orderBy(col(IS_CURRENT_COLUMN).desc(), col(SOURCE_LSN_COLUMN).desc());
            Dataset<Row> mostRecentSilver = access.readSilver(spark)
                    .join(incomingIds, ID_COLUMN)
                    .withColumn(ROW_NUMBER_COLUMN, row_number().over(recency))
                    .filter(col(ROW_NUMBER_COLUMN).equalTo(1))
                    .drop(ROW_NUMBER_COLUMN);

            if (!mostRecentSilver.isEmpty()) {
                Dataset<Row> deletionTimes = dedupedDeletes.select(col(ID_COLUMN), col(DELETION_TIME_COLUMN));
                closedRows = mostRecentSilver.join(deletionTimes, ID_COLUMN)
                        .withColumn(VALID_TO_COLUMN, col(DELETION_TIME_COLUMN))
                        .withColumn(IS_CURRENT_COLUMN, lit(false))
                        .withColumn(IS_DELETED_COLUMN, lit(true))
                        .drop(DELETION_TIME_COLUMN);

                Dataset<Row> existingIds = mostRecentSilver.select(col(ID_COLUMN));
                tombstoneDeletes = dedupedDeletes.join(
                        existingIds,
                        dedupedDeletes.col(ID_COLUMN).equalTo(existingIds.col(ID_COLUMN)),
                        "left_anti");
            }
        }

        Column primaryKey = concat(
                col(ID_COLUMN).cast("string"), lit("-"), col(SOURCE_LSN_COLUMN).cast("string"));
        Dataset<Row> tombstoneRows = tombstoneDeletes
                .withColumn(VALID_FROM_COLUMN, BEGIN_OF_TIME)
                .withColumn(VALID_TO_COLUMN, col(DELETION_TIME_COLUMN))
                .withColumn(IS_CURRENT_COLUMN, lit(true))
                .withColumn(IS_DELETED_COLUMN, lit(true))
                .withColumn(PRIMARY_KEY_COLUMN, primaryKey)
                .drop(DELETION_TIME_COLUMN);

        if (closedRows == null) {
            return tombstoneRows;
        }
        return closedRows.unionByName(tombstoneRows, true);
    }

    private void mergeIntoSilver(
            SparkSession spark,
            Type2TableAccess access,
            Dataset<Row> type2Rows,
            KafkaWriteContext kafka,
            Type2WriteIdentity writeIdentity) {
        String stagingView = stagingViewName(access);
        long rowCount = type2Rows.count();
        type2Rows.createOrReplaceTempView(stagingView);

        try {
            if (!access.silverExists(spark)) {
                log.info("creating silver type-2 table {}", access.tableFqn());
                access.createSilver(spark, stagingView);
            } else {
                ensureIsDeletedColumn(spark, access);
                String updateSetClause = buildMergeUpdateSetClause(type2Rows.columns());
                log.info("merging {} rows into {}", rowCount, access.tableFqn());
                access.mergeSilver(spark, stagingView, PRIMARY_KEY_COLUMN, updateSetClause);
            }
            registerType2Write(access, rowCount, kafka, writeIdentity);
        } finally {
            spark.catalog().dropTempView(stagingView);
        }
    }

    private void registerType2Write(
            Type2TableAccess access, long rowCount, KafkaWriteContext kafka, Type2WriteIdentity writeIdentity) {
        if (access.format() != OpenTableFormat.ICEBERG) {
            return;
        }
        if (writeIdentity == null || !writeIdentity.isComplete()) {
            log.warn(
                    "Skipping datapipelines registration; missing Type 2 write identity for {}",
                    access.tableFqn());
            return;
        }
        try {
            datapipelinesClient.postType2TableWrite(new Type2TableWriteRegistration(
                    writeIdentity.sourceCatalogName(),
                    writeIdentity.sourceDatabaseName(),
                    writeIdentity.sourceNamespaceName(),
                    writeIdentity.sourceTableName(),
                    writeIdentity.databaseName(),
                    writeIdentity.namespaceName(),
                    writeIdentity.tableName(),
                    rowCount,
                    access.warehousePath(),
                    kafka));
        } catch (RuntimeException e) {
            log.error("Failed to register type-2 table write for {}", access.tableFqn(), e);
        }
    }

    /**
     * Builds a staging view name unique to the dataset being transformed so that transforms for
     * different tables can run concurrently in the same Spark session without clobbering each
     * other's staging view.
     */
    private static String stagingViewName(Type2TableAccess access) {
        String sanitizedFqn = access.tableFqn().replaceAll("[^A-Za-z0-9]", "_");
        return TYPE2_STAGING_VIEW_PREFIX + "_" + sanitizedFqn;
    }

    private void ensureIsDeletedColumn(SparkSession spark, Type2TableAccess access) {
        if (Arrays.asList(access.readSilver(spark).columns()).contains(IS_DELETED_COLUMN)) {
            return;
        }
        log.info("adding {} column to silver table {}", IS_DELETED_COLUMN, access.tableFqn());
        access.addIsDeletedColumn(spark, IS_DELETED_COLUMN);
    }

    private Timestamp readMaxUpdatedAt(SparkSession spark, Type2TableAccess access) {
        if (!access.silverExists(spark)) {
            return null;
        }

        Dataset<Row> silver = access.readSilver(spark);
        if (silver.isEmpty()) {
            return null;
        }

        Column watermarkTs = when(col(VALID_TO_COLUMN).equalTo(END_OF_TIME), col(SILVER_UPDATED_AT_COLUMN))
                .otherwise(col(VALID_TO_COLUMN));
        Row row = silver.agg(max(watermarkTs).alias(SILVER_UPDATED_AT_COLUMN)).first();
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
        Column eventTs = when(col(OP_COLUMN).equalTo(DELETE_OP), deletionTime(spark))
                .otherwise(col(SOURCE_UPDATED_AT_COLUMN));
        return source.filter(eventTs.gt(lit(maxUpdatedAt)));
    }

    /**
     * Derives the delete event time from Debezium {@code ts_ms}. The bronze pipeline parses ISO
     * timestamps like {@code updated_at} as naive wall-clock values in the session time zone (the
     * trailing {@code Z} is treated as a literal). To keep delete events comparable with those
     * values, the UTC instant from {@code ts_ms} is re-anchored to the same session-local clock.
     */
    private Column deletionTime(SparkSession spark) {
        String sessionTimeZone = spark.conf().get("spark.sql.session.timeZone");
        return to_utc_timestamp(timestamp_millis(col(TS_MS_COLUMN)), sessionTimeZone);
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

    private Dataset<Row> transformDeletesForType2(Dataset<Row> df) {
        List<Column> columns = new ArrayList<>();
        for (String name : df.columns()) {
            if (name.startsWith(AFTER_PREFIX)) {
                continue;
            }
            if (name.startsWith(BEFORE_PREFIX)) {
                columns.add(col(name).alias(name.substring(BEFORE_PREFIX.length())));
            } else {
                columns.add(col(name));
            }
        }
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
