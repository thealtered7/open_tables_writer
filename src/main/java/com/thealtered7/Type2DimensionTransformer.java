package com.thealtered7;

import com.thealtered7.datapipelines.DatapipelinesClient;
import com.thealtered7.datapipelines.KafkaWriteContext;
import com.thealtered7.datapipelines.NoopDatapipelinesClient;
import com.thealtered7.datapipelines.TableWriteRegistration;
import com.thealtered7.models.TableUpdatedNotification.OpenTableFormat;
import com.thealtered7.observability.Observability;
import com.thealtered7.schema.IcebergSchemaEvolver;
import com.thealtered7.schemaregistry.SchemaRegistryConfig;
import com.thealtered7.schemaregistry.TableSchemaRegistrar;
import com.thealtered7.schemaregistry.TableSchemaRegistrars;
import com.thealtered7.schemaregistry.TableSchemaSubjects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.concat_ws;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.current_timestamp;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.lead;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.row_number;
import static org.apache.spark.sql.functions.sha2;
import static org.apache.spark.sql.functions.timestamp_millis;
import static org.apache.spark.sql.functions.to_utc_timestamp;
import static org.apache.spark.sql.functions.when;

import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
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
    private static final String SILVER_UPDATED_AT_COLUMN = "updated_at";
    private static final String BEFORE_PREFIX = "before_";
    private static final String AFTER_PREFIX = "after_";
    private static final String ID_COLUMN = "id";
    private static final String SOURCE_LSN_COLUMN = "_source_lsn";
    private static final String EXTRACT_JOB_ID_COLUMN = "_extract_job_id";
    private static final String EXTRACT_BUFFER_ID_COLUMN = "_extract_buffer_id";
    private static final String EXTRACT_TYPE_COLUMN = "_extract_type";
    private static final String EXTRACTED_AT_COLUMN = "_extracted_at";
    private static final String TRANSFORMED_AT_COLUMN = "_transformed_at";
    private static final String PRIMARY_KEY_COLUMN = "primary_key";
    private static final String VERSION_KEY_COLUMN = "version_key";
    private static final int SHA256_BITS = 256;
    private static final String VALID_FROM_COLUMN = "_valid_from";
    private static final String VALID_TO_COLUMN = "_valid_to";
    private static final String IS_CURRENT_COLUMN = "_is_current";
    private static final String IS_DELETED_COLUMN = "_is_deleted";
    private static final String RECORD_NUMBER_COLUMN = "_record_number";
    private static final String RECORD_COUNT_COLUMN = "_record_count";
    private static final String OP_COLUMN = "_op";
    private static final String TS_MS_COLUMN = "_ts_ms";
    private static final String DELETE_OP = "d";
    private static final String DELETION_TIME_COLUMN = "deletion_time";
    private static final String ROW_NUMBER_COLUMN = "row_number";
    private static final String TYPE2_STAGING_VIEW_PREFIX = "type2_staging";
    private static final String TYPE1_STAGING_VIEW_PREFIX = "type1_staging";
    private static final String NEXT_UPDATED_AT_COLUMN = "next_updated_at";
    private static final String END_OF_TIME_LITERAL = "10000-12-31 23:59:59.999999";
    private static final String BEGIN_OF_TIME_LITERAL = "1970-01-01 00:00:00";
    private static final Column END_OF_TIME = expr("timestamp '" + END_OF_TIME_LITERAL + "'");
    private static final Column BEGIN_OF_TIME = expr("timestamp '" + BEGIN_OF_TIME_LITERAL + "'");
    private static final String NATURAL_KEY_COLUMN = "natural_key";
    /** SCD attributes preserved on closed history rows (record number/count are recomputed). */
    private static final List<String> PRESERVED_TYPE2_COLUMNS = List.of(
            VALID_FROM_COLUMN,
            VALID_TO_COLUMN,
            IS_CURRENT_COLUMN,
            PRIMARY_KEY_COLUMN,
            VERSION_KEY_COLUMN,
            IS_DELETED_COLUMN,
            NATURAL_KEY_COLUMN);
    private static final Set<String> TYPE2_COLUMNS = Set.of(
            VALID_FROM_COLUMN,
            VALID_TO_COLUMN,
            IS_CURRENT_COLUMN,
            PRIMARY_KEY_COLUMN,
            VERSION_KEY_COLUMN,
            IS_DELETED_COLUMN,
            RECORD_NUMBER_COLUMN,
            RECORD_COUNT_COLUMN);

    private final Observability observability;
    private final DatapipelinesClient datapipelinesClient;
    private final TableSchemaRegistrar tableSchemaRegistrar;

    public Type2DimensionTransformer() {
        this(Observability.noop());
    }

    public Type2DimensionTransformer(Observability observability) {
        this(observability, new NoopDatapipelinesClient());
    }

    public Type2DimensionTransformer(Observability observability, DatapipelinesClient datapipelinesClient) {
        this(observability, datapipelinesClient, TableSchemaRegistrars.create(SchemaRegistryConfig.none()));
    }

    public Type2DimensionTransformer(
            Observability observability,
            DatapipelinesClient datapipelinesClient,
            TableSchemaRegistrar tableSchemaRegistrar) {
        this.observability = Objects.requireNonNull(observability, "observability");
        this.datapipelinesClient = Objects.requireNonNull(datapipelinesClient, "datapipelinesClient");
        this.tableSchemaRegistrar =
                Objects.requireNonNull(tableSchemaRegistrar, "tableSchemaRegistrar");
    }

    public void transform(SparkSession spark, TableIdentity table, String extractBufferId) {
        Path silverWarehouse = Path.of(spark.conf().get("spark.sql.catalog.silver_catalog.warehouse"));
        Type2WriteIdentity bufferIdentity = bufferScopedIdentity(extractBufferId);
        transform(spark, new IcebergType2TableAccess(table, silverWarehouse), null, bufferIdentity, null);
    }

    public void transform(SparkSession spark, Type2TableAccess access, String extractBufferId) {
        transform(spark, access, null, bufferScopedIdentity(extractBufferId), null);
    }

    public void transform(SparkSession spark, Type2TableAccess access, KafkaWriteContext kafka) {
        transform(spark, access, kafka, null, null);
    }

    public void transform(
            SparkSession spark, Type2TableAccess access, KafkaWriteContext kafka, Type2WriteIdentity writeIdentity) {
        transform(spark, access, kafka, writeIdentity, null);
    }

    public void transform(
            SparkSession spark,
            Type2TableAccess access,
            KafkaWriteContext kafka,
            Type2WriteIdentity type2WriteIdentity,
            Type1WriteIdentity type1WriteIdentity) {
        try {
            Map<String, String> tags = new HashMap<>();
            tags.put("table", access.tableFqn());
            if (type2WriteIdentity != null) {
                tags.put(ExtractMdc.EXTRACT_JOB_ID, ExtractMdc.normalize(type2WriteIdentity.extractJobId()));
                tags.put(ExtractMdc.EXTRACT_BUFFER_ID, ExtractMdc.normalize(type2WriteIdentity.extractBufferId()));
            } else if (type1WriteIdentity != null) {
                tags.put(ExtractMdc.EXTRACT_JOB_ID, ExtractMdc.normalize(type1WriteIdentity.extractJobId()));
                tags.put(ExtractMdc.EXTRACT_BUFFER_ID, ExtractMdc.normalize(type1WriteIdentity.extractBufferId()));
            }
            observability.observeCallableVoid(
                    Observability.TYPE2_DIMENSION_TRANSFORMER_PREFIX,
                    "transform",
                    tags,
                    () -> {
                        transformInternal(spark, access, kafka, type2WriteIdentity, type1WriteIdentity);
                        return "success";
                    });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void transformInternal(
            SparkSession spark,
            Type2TableAccess access,
            KafkaWriteContext kafka,
            Type2WriteIdentity type2WriteIdentity,
            Type1WriteIdentity type1WriteIdentity) {
        log.info("transforming table: {}", access.tableFqn());
        String extractBufferId = resolveExtractBufferId(type2WriteIdentity, type1WriteIdentity);
        if (extractBufferId == null) {
            log.warn("Skipping transform; missing extract_buffer_id for {}", access.tableFqn());
            return;
        }
        Dataset<Row> source = readSourceTable(spark, access, extractBufferId);
        if (source.isEmpty()) {
            throw new IllegalStateException(String.format(
                    "No bronze rows for extract_buffer_id=%s on %s after catalog refresh",
                    extractBufferId, access.tableFqn()));
        }

        Dataset<Row> deletes = source.filter(col(OP_COLUMN).equalTo(DELETE_OP));
        Dataset<Row> upserts = source.filter(col(OP_COLUMN).notEqual(DELETE_OP).or(col(OP_COLUMN).isNull()));

        Dataset<Row> result = null;
        if (!upserts.isEmpty()) {
            Dataset<Row> transformedUpserts = transformForType2(upserts);
            Dataset<Row> changedUpserts = filterChangedUpdatedAt(spark, access, transformedUpserts);
            Dataset<Row> lineageRefresh = buildExtractLineageRefresh(spark, access, transformedUpserts);

            Dataset<Row> type2Upserts = null;
            if (!changedUpserts.isEmpty()) {
                type2Upserts = buildType2Rows(spark, access, changedUpserts);
            }
            if (!lineageRefresh.isEmpty()) {
                type2Upserts = type2Upserts == null
                        ? lineageRefresh
                        : type2Upserts.unionByName(lineageRefresh, true);
            }
            if (type2Upserts != null && !type2Upserts.isEmpty()) {
                result = type2Upserts;
            }
        }
        if (!deletes.isEmpty()) {
            Dataset<Row> deleteRows = buildDeleteType2Rows(spark, access, deletes);
            result = result == null ? deleteRows : result.unionByName(deleteRows, true);
        }
        if (result == null || result.isEmpty()) {
            log.info("no upsert or delete rows to apply for {}", access.tableFqn());
            return;
        }
        mergeIntoSilver(spark, access, result, kafka, type2WriteIdentity, type1WriteIdentity);
    }

    private Dataset<Row> buildType2Rows(SparkSession spark, Type2TableAccess access, Dataset<Row> incoming) {
        String[] baseColumns = baseColumnNames(incoming);
        Dataset<Row> ids = incoming.select(col(ID_COLUMN)).distinct();
        Dataset<Row> tipAndIncoming = incoming.select(toColumns(baseColumns));
        Dataset<Row> historyRows = null;

        if (access.silverExists(spark)) {
            Dataset<Row> incomingKeys = incoming
                    .select(col(ID_COLUMN).alias("incoming_id"), col(SOURCE_LSN_COLUMN).alias("incoming_lsn"))
                    .distinct();
            // Full PK-group history so _record_count can be rewritten on older versions.
            Dataset<Row> silverForIds = access.readSilver(spark)
                    .join(ids, ID_COLUMN)
                    .join(
                            incomingKeys,
                            col(ID_COLUMN)
                                    .equalTo(col("incoming_id"))
                                    .and(col(SOURCE_LSN_COLUMN).equalTo(col("incoming_lsn"))),
                            "left_anti");
            if (!silverForIds.isEmpty()) {
                Dataset<Row> currentSilver = silverForIds.filter(col(IS_CURRENT_COLUMN).equalTo(true));
                Dataset<Row> closedHistory = silverForIds.filter(col(IS_CURRENT_COLUMN).equalTo(false));
                if (!currentSilver.isEmpty()) {
                    // Incoming may include columns not yet on silver (evolution runs in mergeIntoSilver).
                    // Null-fill so select/unionByName do not fail with UNRESOLVED_COLUMN.
                    tipAndIncoming = tipAndIncoming.unionByName(
                            selectAlignedToIncoming(currentSilver, incoming, baseColumns));
                }
                if (!closedHistory.isEmpty()) {
                    // Preserve SCD attrs; do not re-window validity (would reopen deleted tips).
                    historyRows = selectAlignedHistory(closedHistory, incoming, baseColumns);
                }
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

        Dataset<Row> scdRows = tipAndIncoming
                .withColumn(NEXT_UPDATED_AT_COLUMN, nextUpdatedAt)
                .withColumn(VALID_FROM_COLUMN, validFrom)
                .withColumn(VALID_TO_COLUMN, validTo)
                .withColumn(IS_CURRENT_COLUMN, isCurrent)
                .withColumn(PRIMARY_KEY_COLUMN, primaryKey)
                .withColumn(VERSION_KEY_COLUMN, versionKey())
                .withColumn(IS_DELETED_COLUMN, lit(false))
                .drop(NEXT_UPDATED_AT_COLUMN);

        Dataset<Row> combined =
                historyRows == null ? scdRows : scdRows.unionByName(historyRows, true);
        // Derive after history union so closed versions are not MERGEd with null natural_key.
        return withRecordNumbering(combined.withColumn(NATURAL_KEY_COLUMN, col(ID_COLUMN)));
    }

    /**
     * Keeps upserts whose {@code updated_at} differs from the current silver row (or that have no
     * current silver row). Same {@code updated_at} is handled by {@link #buildExtractLineageRefresh}.
     */
    private Dataset<Row> filterChangedUpdatedAt(
            SparkSession spark, Type2TableAccess access, Dataset<Row> incoming) {
        if (!access.silverExists(spark)) {
            return incoming;
        }
        Dataset<Row> currentUpdatedAt = access.readSilver(spark)
                .filter(col(IS_CURRENT_COLUMN).equalTo(true))
                .select(
                        col(ID_COLUMN).alias("current_id"),
                        col(SILVER_UPDATED_AT_COLUMN).alias("current_updated_at"));
        return incoming.join(
                currentUpdatedAt,
                col(ID_COLUMN)
                        .equalTo(col("current_id"))
                        .and(col(SILVER_UPDATED_AT_COLUMN).equalTo(col("current_updated_at"))),
                "left_anti");
    }

    /**
     * For upserts whose {@code updated_at} matches the current silver row, returns that silver row
     * with extract lineage columns overwritten from the incoming buffer (no new SCD2 version).
     */
    private Dataset<Row> buildExtractLineageRefresh(
            SparkSession spark, Type2TableAccess access, Dataset<Row> incoming) {
        if (!access.silverExists(spark)) {
            return incoming.limit(0);
        }
        boolean hasExtractedAt =
                Arrays.asList(incoming.columns()).contains(EXTRACTED_AT_COLUMN);
        List<Column> extractSelect = new ArrayList<>();
        extractSelect.add(col(ID_COLUMN).alias("incoming_id"));
        extractSelect.add(col(SILVER_UPDATED_AT_COLUMN).alias("incoming_updated_at"));
        extractSelect.add(col(EXTRACT_JOB_ID_COLUMN).alias("incoming_extract_job_id"));
        extractSelect.add(col(EXTRACT_BUFFER_ID_COLUMN).alias("incoming_extract_buffer_id"));
        extractSelect.add(col(EXTRACT_TYPE_COLUMN).alias("incoming_extract_type"));
        if (hasExtractedAt) {
            extractSelect.add(col(EXTRACTED_AT_COLUMN).alias("incoming_extracted_at"));
        }
        Dataset<Row> incomingExtract = incoming.select(extractSelect.toArray(Column[]::new));
        Dataset<Row> currentSilver = access.readSilver(spark).filter(col(IS_CURRENT_COLUMN).equalTo(true));
        Dataset<Row> refreshed = currentSilver
                .join(
                        incomingExtract,
                        col(ID_COLUMN)
                                .equalTo(col("incoming_id"))
                                .and(col(SILVER_UPDATED_AT_COLUMN).equalTo(col("incoming_updated_at"))))
                .withColumn(EXTRACT_JOB_ID_COLUMN, col("incoming_extract_job_id"))
                .withColumn(EXTRACT_BUFFER_ID_COLUMN, col("incoming_extract_buffer_id"))
                .withColumn(EXTRACT_TYPE_COLUMN, col("incoming_extract_type"));
        if (hasExtractedAt) {
            refreshed = refreshed.withColumn(EXTRACTED_AT_COLUMN, col("incoming_extracted_at"));
        }
        List<String> dropCols = new ArrayList<>(List.of(
                "incoming_id",
                "incoming_updated_at",
                "incoming_extract_job_id",
                "incoming_extract_buffer_id",
                "incoming_extract_type"));
        if (hasExtractedAt) {
            dropCols.add("incoming_extracted_at");
        }
        return refreshed.drop(dropCols.toArray(String[]::new));
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
                        .withColumn(VERSION_KEY_COLUMN, versionKey())
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
                .withColumn(NATURAL_KEY_COLUMN, col(ID_COLUMN))
                .withColumn(VERSION_KEY_COLUMN, versionKey())
                .withColumn(RECORD_NUMBER_COLUMN, lit(1))
                .withColumn(RECORD_COUNT_COLUMN, lit(1))
                .drop(DELETION_TIME_COLUMN);

        if (closedRows == null) {
            return tombstoneRows;
        }
        // Closing a tip does not change PK-group size; keep existing record number/count.
        return ensureRecordColumnsPreserved(closedRows).unionByName(tombstoneRows, true);
    }

    private void mergeIntoSilver(
            SparkSession spark,
            Type2TableAccess access,
            Dataset<Row> type2Rows,
            KafkaWriteContext kafka,
            Type2WriteIdentity type2WriteIdentity,
            Type1WriteIdentity type1WriteIdentity) {
        String valueSchema = type2WriteIdentity == null ? null : type2WriteIdentity.valueSchema();
        if (access.silverExists(spark)) {
            ensureIsDeletedColumn(spark, access);
            ensureVersionKeyColumn(spark, access);
        }
        Dataset<Row> withTransformedAt = type2Rows.withColumn(TRANSFORMED_AT_COLUMN, current_timestamp());
        Dataset<Row> evolved = IcebergSchemaEvolver.evolveAndAlign(
                spark,
                access.silverCatalogTableName(),
                access.silverSqlTableName(),
                withTransformedAt,
                valueSchema,
                IcebergSchemaEvolver.LayerMode.SILVER);

        String stagingView = stagingViewName(access);
        long rowCount = evolved.count();
        evolved.createOrReplaceTempView(stagingView);

        try {
            Instant mergeStartAt = Instant.now();
            if (!access.silverExists(spark)) {
                log.info("creating silver type-2 table {}", access.tableFqn());
                access.createSilver(spark, stagingView);
            } else {
                String updateSetClause = buildMergeUpdateSetClause(evolved.columns(), VERSION_KEY_COLUMN);
                log.info("merging {} rows into {}", rowCount, access.tableFqn());
                access.mergeSilver(spark, stagingView, VERSION_KEY_COLUMN, updateSetClause);
            }
            Instant mergeEndAt = Instant.now();
            spark.catalog().refreshTable(access.silverCatalogTableName());
            String tableSchemaJson = spark.table(access.silverCatalogTableName()).schema().json();
            String tableSchemaId = null;
            if (type2WriteIdentity != null && type2WriteIdentity.isComplete()) {
                String subject = TableSchemaSubjects.valueSubject(
                        type2WriteIdentity.databaseName(),
                        type2WriteIdentity.namespaceName(),
                        type2WriteIdentity.tableName());
                tableSchemaId = tableSchemaRegistrar.register(subject, tableSchemaJson);
            }
            registerType2Write(
                    access,
                    rowCount,
                    kafka,
                    type2WriteIdentity,
                    mergeStartAt,
                    mergeEndAt,
                    tableSchemaJson,
                    tableSchemaId);
            mergeIntoType1(spark, access, evolved, kafka, type1WriteIdentity, valueSchema);
        } finally {
            spark.catalog().dropTempView(stagingView);
        }
    }

    private void mergeIntoType1(
            SparkSession spark,
            Type2TableAccess access,
            Dataset<Row> type2Rows,
            KafkaWriteContext kafka,
            Type1WriteIdentity type1WriteIdentity,
            String valueSchema) {
        WindowSpec latestPerId = Window.partitionBy(ID_COLUMN).orderBy(col(SOURCE_LSN_COLUMN).desc());
        Dataset<Row> type1Rows = type2Rows
                .filter(col(IS_CURRENT_COLUMN).or(col(IS_DELETED_COLUMN)))
                .withColumn(ROW_NUMBER_COLUMN, row_number().over(latestPerId))
                .filter(col(ROW_NUMBER_COLUMN).equalTo(1))
                .drop(ROW_NUMBER_COLUMN)
                .drop(IS_CURRENT_COLUMN, VALID_FROM_COLUMN, VALID_TO_COLUMN);

        if (type1Rows.isEmpty()) {
            log.info("no type-1 rows to apply for {}", access.tableFqn());
            return;
        }

        Dataset<Row> withTransformedAt = type1Rows.withColumn(TRANSFORMED_AT_COLUMN, current_timestamp());
        Dataset<Row> evolvedType1 = IcebergSchemaEvolver.evolveAndAlign(
                spark,
                access.type1CatalogTableName(),
                access.type1SqlTableName(),
                withTransformedAt,
                valueSchema,
                IcebergSchemaEvolver.LayerMode.SILVER);

        String stagingView = type1StagingViewName(access);
        long rowCount = evolvedType1.count();
        evolvedType1.createOrReplaceTempView(stagingView);

        try {
            Instant mergeStartAt = Instant.now();
            if (!access.type1Exists(spark)) {
                log.info("creating silver type-1 table for {}", access.tableFqn());
                access.createType1(spark, stagingView);
            } else {
                String updateSetClause = buildMergeUpdateSetClause(evolvedType1.columns(), ID_COLUMN);
                log.info("merging {} type-1 rows for {}", rowCount, access.tableFqn());
                access.mergeType1(spark, stagingView, ID_COLUMN, updateSetClause);
            }
            Instant mergeEndAt = Instant.now();
            spark.catalog().refreshTable(access.type1CatalogTableName());
            String tableSchemaJson = spark.table(access.type1CatalogTableName()).schema().json();
            String tableSchemaId = null;
            if (type1WriteIdentity != null && type1WriteIdentity.isComplete()) {
                String subject = TableSchemaSubjects.valueSubject(
                        type1WriteIdentity.databaseName(),
                        type1WriteIdentity.namespaceName(),
                        type1WriteIdentity.tableName());
                tableSchemaId = tableSchemaRegistrar.register(subject, tableSchemaJson);
            }
            registerType1Write(
                    access,
                    rowCount,
                    kafka,
                    type1WriteIdentity,
                    mergeStartAt,
                    mergeEndAt,
                    tableSchemaJson,
                    tableSchemaId);
        } finally {
            spark.catalog().dropTempView(stagingView);
        }
    }

    private void registerType2Write(
            Type2TableAccess access,
            long rowCount,
            KafkaWriteContext kafka,
            Type2WriteIdentity writeIdentity,
            Instant mergeStartAt,
            Instant mergeEndAt,
            String tableSchemaJson,
            String tableSchemaId) {
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
            datapipelinesClient.postTableWrite(new TableWriteRegistration(
                    TableWriteRegistration.WRITE_TYPE_SILVER_TYPE_2,
                    writeIdentity.databaseName(),
                    writeIdentity.namespaceName(),
                    writeIdentity.tableName(),
                    writeIdentity.sourceInstanceName(),
                    writeIdentity.sourceDatabaseName(),
                    writeIdentity.sourceSchemaName(),
                    writeIdentity.sourceTableName(),
                    rowCount,
                    rowCount,
                    writeIdentity.rawFilePath(),
                    writeIdentity.rawFileSize(),
                    writeIdentity.extractJobId(),
                    writeIdentity.extractBufferId(),
                    writeIdentity.extractType(),
                    writeIdentity.extractStartAt(),
                    writeIdentity.extractEndAt(),
                    mergeStartAt,
                    mergeEndAt,
                    access.warehousePath(),
                    kafka,
                    null,
                    tableSchemaJson,
                    null,
                    tableSchemaId,
                    writeIdentity.sourceMinLsn(),
                    writeIdentity.sourceMaxLsn()));
        } catch (RuntimeException e) {
            log.error("Failed to register type-2 table write for {}", access.tableFqn(), e);
        }
    }

    private void registerType1Write(
            Type2TableAccess access,
            long rowCount,
            KafkaWriteContext kafka,
            Type1WriteIdentity writeIdentity,
            Instant mergeStartAt,
            Instant mergeEndAt,
            String tableSchemaJson,
            String tableSchemaId) {
        if (access.format() != OpenTableFormat.ICEBERG) {
            return;
        }
        if (writeIdentity == null || !writeIdentity.isComplete()) {
            log.warn(
                    "Skipping datapipelines registration; missing Type 1 write identity for {}",
                    access.tableFqn());
            return;
        }
        try {
            datapipelinesClient.postTableWrite(new TableWriteRegistration(
                    TableWriteRegistration.WRITE_TYPE_SILVER_TYPE_1,
                    writeIdentity.databaseName(),
                    writeIdentity.namespaceName(),
                    writeIdentity.tableName(),
                    writeIdentity.sourceInstanceName(),
                    writeIdentity.sourceDatabaseName(),
                    writeIdentity.sourceSchemaName(),
                    writeIdentity.sourceTableName(),
                    rowCount,
                    rowCount,
                    writeIdentity.rawFilePath(),
                    writeIdentity.rawFileSize(),
                    writeIdentity.extractJobId(),
                    writeIdentity.extractBufferId(),
                    writeIdentity.extractType(),
                    writeIdentity.extractStartAt(),
                    writeIdentity.extractEndAt(),
                    mergeStartAt,
                    mergeEndAt,
                    access.warehousePath(),
                    kafka,
                    null,
                    tableSchemaJson,
                    null,
                    tableSchemaId,
                    writeIdentity.sourceMinLsn(),
                    writeIdentity.sourceMaxLsn()));
        } catch (RuntimeException e) {
            log.error("Failed to register type-1 table write for {}", access.tableFqn(), e);
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

    private static String type1StagingViewName(Type2TableAccess access) {
        String sanitizedFqn = access.tableFqn().replaceAll("[^A-Za-z0-9]", "_");
        return TYPE1_STAGING_VIEW_PREFIX + "_" + sanitizedFqn;
    }

    private void ensureIsDeletedColumn(SparkSession spark, Type2TableAccess access) {
        if (Arrays.asList(access.readSilver(spark).columns()).contains(IS_DELETED_COLUMN)) {
            return;
        }
        log.info("adding {} column to silver table {}", IS_DELETED_COLUMN, access.tableFqn());
        access.addIsDeletedColumn(spark, IS_DELETED_COLUMN);
    }

    private void ensureVersionKeyColumn(SparkSession spark, Type2TableAccess access) {
        if (Arrays.asList(access.readSilver(spark).columns()).contains(VERSION_KEY_COLUMN)) {
            return;
        }
        log.info("adding {} column to silver table {}", VERSION_KEY_COLUMN, access.tableFqn());
        access.addVersionKeyColumn(spark, VERSION_KEY_COLUMN, PRIMARY_KEY_COLUMN, VALID_FROM_COLUMN);
    }

    private static String resolveExtractBufferId(
            Type2WriteIdentity type2WriteIdentity, Type1WriteIdentity type1WriteIdentity) {
        if (type2WriteIdentity != null && isPresent(type2WriteIdentity.extractBufferId())) {
            return type2WriteIdentity.extractBufferId();
        }
        if (type1WriteIdentity != null && isPresent(type1WriteIdentity.extractBufferId())) {
            return type1WriteIdentity.extractBufferId();
        }
        return null;
    }

    private static Type2WriteIdentity bufferScopedIdentity(String extractBufferId) {
        return new Type2WriteIdentity(
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                extractBufferId,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null);
    }

    private static boolean isPresent(String value) {
        return value != null && !value.isBlank();
    }

    private Dataset<Row> readSourceTable(SparkSession spark, Type2TableAccess access, String extractBufferId) {
        return access.readBronze(spark).filter(col(EXTRACT_BUFFER_ID_COLUMN).equalTo(lit(extractBufferId)));
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

    /**
     * Projects {@code baseColumns} from {@code rows}, adding null-typed columns for any name present
     * on {@code schemaSource} but missing from {@code rows} (incoming schema ahead of silver).
     */
    static Dataset<Row> selectAlignedToIncoming(
            Dataset<Row> rows, Dataset<Row> schemaSource, String[] baseColumns) {
        Set<String> present = Arrays.stream(rows.columns()).collect(Collectors.toSet());
        Dataset<Row> aligned = rows;
        for (String name : baseColumns) {
            if (!present.contains(name)) {
                aligned = aligned.withColumn(
                        name, lit(null).cast(schemaSource.schema().apply(name).dataType()));
            }
        }
        return aligned.select(toColumns(baseColumns));
    }

    /**
     * Aligns closed history rows to the incoming base schema while preserving SCD attributes.
     * Record number/count are dropped so they can be recomputed over the full PK group.
     */
    static Dataset<Row> selectAlignedHistory(
            Dataset<Row> rows, Dataset<Row> schemaSource, String[] baseColumns) {
        Set<String> present = Arrays.stream(rows.columns()).collect(Collectors.toSet());
        Dataset<Row> aligned = rows;
        for (String name : baseColumns) {
            if (!present.contains(name)) {
                aligned = aligned.withColumn(
                        name, lit(null).cast(schemaSource.schema().apply(name).dataType()));
            }
        }
        if (present.contains(RECORD_NUMBER_COLUMN)) {
            aligned = aligned.drop(RECORD_NUMBER_COLUMN);
        }
        if (present.contains(RECORD_COUNT_COLUMN)) {
            aligned = aligned.drop(RECORD_COUNT_COLUMN);
        }
        List<String> columns = new ArrayList<>(Arrays.asList(baseColumns));
        columns.addAll(PRESERVED_TYPE2_COLUMNS);
        return aligned.select(toColumns(columns.toArray(String[]::new)));
    }

    private static Dataset<Row> withRecordNumbering(Dataset<Row> rows) {
        WindowSpec ordered = Window.partitionBy(ID_COLUMN).orderBy(col(SOURCE_LSN_COLUMN));
        WindowSpec partition = Window.partitionBy(ID_COLUMN);
        return rows.withColumn(RECORD_NUMBER_COLUMN, row_number().over(ordered))
                .withColumn(RECORD_COUNT_COLUMN, count(lit(1)).over(partition).cast("int"));
    }

    /** Ensures delete-close rows keep {@code _record_number}/{@code _record_count} when present. */
    private static Dataset<Row> ensureRecordColumnsPreserved(Dataset<Row> rows) {
        Set<String> present = Arrays.stream(rows.columns()).collect(Collectors.toSet());
        Dataset<Row> withCols = rows;
        if (!present.contains(RECORD_NUMBER_COLUMN)) {
            withCols = withCols.withColumn(RECORD_NUMBER_COLUMN, lit(1));
        }
        if (!present.contains(RECORD_COUNT_COLUMN)) {
            withCols = withCols.withColumn(RECORD_COUNT_COLUMN, lit(1));
        }
        return withCols;
    }

    private static Column[] toColumns(String[] columnNames) {
        return Arrays.stream(columnNames).map(org.apache.spark.sql.functions::col).toArray(Column[]::new);
    }

    private static Column versionKey() {
        return sha2(
                concat_ws("|", col(PRIMARY_KEY_COLUMN), col(VALID_FROM_COLUMN).cast("string")), SHA256_BITS);
    }

    private static String buildMergeUpdateSetClause(String[] columns, String onColumn) {
        return Arrays.stream(columns)
                .filter(column -> !onColumn.equals(column))
                .map(column -> String.format("t.`%s` = s.`%s`", column, column))
                .collect(Collectors.joining(", "));
    }
}
