package com.thealtered7;

import com.thealtered7.observability.Observability;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.to_timestamp;

public final class DebeziumPayloadFlattener {

    private static final Logger log = LoggerFactory.getLogger(DebeziumPayloadFlattener.class);
    private static final Set<String> PREFIXED_STRUCTS = Set.of("before", "after", "source");
    private static final Set<String> TOP_LEVEL_FIELDS = Set.of("op", "transaction", "ts_ms", "ts_ns", "ts_us");
    private static final Set<String> BUSINESS_IMAGE_PREFIXES = Set.of("before_", "after_");
    private static final String EXTRACT_STRUCT = "extract";
    private static final String ISO_TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss[.SSSSSS]'Z'";

    private final Observability observability;

    public DebeziumPayloadFlattener() {
        this(Observability.noop());
    }

    public DebeziumPayloadFlattener(Observability observability) {
        this.observability = Objects.requireNonNull(observability, "observability");
    }

    public Dataset<Row> loadJsonLines(SparkSession spark, Path inputFilePath) {
        try {
            return observability.observeCallable(
                    Observability.DEBEZIUM_PAYLOAD_FLATTENER_PREFIX,
                    "load_json_lines",
                    Map.of("input_file", inputFilePath.toString()),
                    () -> {
                        InputFileWaiter.requireFile(inputFilePath);
                        return spark.read().json(inputFilePath.toString());
                    });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Dataset<Row> flattenPayload(Dataset<Row> raw) {
        try {
            return observability.observeCallable(
                    Observability.DEBEZIUM_PAYLOAD_FLATTENER_PREFIX,
                    "flatten_payload",
                    Collections.emptyMap(),
                    () -> flattenPayloadInternal(raw));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Dataset<Row> flattenPayloadInternal(Dataset<Row> raw) {
        StructType payloadSchema = (StructType) raw.schema().apply("payload").dataType();

        StructType rowDataSchema = resolveRowDataSchema(payloadSchema);

        List<Column> columns = new ArrayList<>();
        for (StructField field : payloadSchema.fields()) {
            String name = field.name();
            if (PREFIXED_STRUCTS.contains(name)) {
                if (field.dataType() instanceof StructType structType) {
                    expandPayloadStruct(columns, name, structType);
                } else if (("before".equals(name) || "after".equals(name)) && rowDataSchema != null) {
                    expandNullStruct(columns, name, rowDataSchema);
                } else {
                    log.warn("Skipping non-struct payload field {} of type {}", name, field.dataType());
                }
            } else if (TOP_LEVEL_FIELDS.contains(name)) {
                columns.add(col("payload." + name).alias(metadataAlias(name)));
            }
        }

        expandExtractStruct(raw, columns);

        return raw.select(columns.toArray(Column[]::new));
    }

    private void expandExtractStruct(Dataset<Row> raw, List<Column> columns) {
        try {
            StructField extractField = raw.schema().apply(EXTRACT_STRUCT);
            if (!(extractField.dataType() instanceof StructType extractSchema)) {
                return;
            }
            for (StructField nested : extractSchema.fields()) {
                String alias = metadataAlias(nested.name());
                columns.add(col(EXTRACT_STRUCT + "." + nested.name()).alias(alias));
            }
        } catch (IllegalArgumentException ignored) {
            // extract is optional for older/raw fixtures without the wrapper
        }
    }

    private StructType resolveRowDataSchema(StructType payloadSchema) {
        StructType afterSchema = structFieldType(payloadSchema, "after");
        return afterSchema != null ? afterSchema : structFieldType(payloadSchema, "before");
    }

    private StructType structFieldType(StructType payloadSchema, String name) {
        for (StructField field : payloadSchema.fields()) {
            if (field.name().equals(name) && field.dataType() instanceof StructType structType) {
                return structType;
            }
        }
        return null;
    }

    private void expandPayloadStruct(List<Column> columns, String structName, StructType structType) {
        for (StructField nested : structType.fields()) {
            String alias = structName + "_" + nested.name();
            if (!isBusinessImageColumn(alias)) {
                alias = metadataAlias(alias);
            }
            columns.add(col("payload." + structName + "." + nested.name()).alias(alias));
        }
    }

    private void expandNullStruct(List<Column> columns, String structName, StructType structType) {
        for (StructField nested : structType.fields()) {
            String alias = structName + "_" + nested.name();
            if (!isBusinessImageColumn(alias)) {
                alias = metadataAlias(alias);
            }
            columns.add(lit(null).cast(nested.dataType()).alias(alias));
        }
    }

    private static boolean isBusinessImageColumn(String name) {
        for (String prefix : BUSINESS_IMAGE_PREFIXES) {
            if (name.startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }

    private static String metadataAlias(String name) {
        return name.startsWith("_") ? name : "_" + name;
    }

    public Dataset<Row> convertTimestampColumns(Dataset<Row> flat) {
        try {
            return observability.observeCallable(
                    Observability.DEBEZIUM_PAYLOAD_FLATTENER_PREFIX,
                    "convert_timestamp_columns",
                    Collections.emptyMap(),
                    () -> convertTimestampColumnsInternal(flat));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Dataset<Row> convertTimestampColumnsInternal(Dataset<Row> flat) {
        List<Column> columns = new ArrayList<>();
        for (StructField field : flat.schema().fields()) {
            String name = field.name();
            if (isTimestampColumn(name) && field.dataType().equals(DataTypes.StringType)) {
                columns.add(to_timestamp(col(name), ISO_TIMESTAMP_FORMAT).alias(name));
            } else {
                columns.add(col(name));
            }
        }
        return flat.select(columns.toArray(Column[]::new));
    }

    private boolean isTimestampColumn(String name) {
        return name.endsWith("_created_at")
                || name.endsWith("_updated_at")
                || name.equals("_extracted_at");
    }

    public Path getOutputTablePath(String tableFQN, Path dataDirectoryBasePath) {
        try {
            return observability.observeCallable(
                    Observability.DEBEZIUM_PAYLOAD_FLATTENER_PREFIX,
                    "get_output_table_path",
                    Map.of("table", tableFQN),
                    () -> getOutputTablePathInternal(tableFQN, dataDirectoryBasePath));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Path getOutputTablePathInternal(String tableFQN, Path dataDirectoryBasePath) {
        String[] tableParts = tableFQN.split("\\.");
        String instance = tableParts[0];
        String schema = tableParts[1];
        String table = tableParts[2];
        return dataDirectoryBasePath
                .resolve(instance)
                .resolve(schema)
                .resolve(table);
    }

    public DatePartition getDatePartition(Date now) {
        try {
            return observability.observeCallable(
                    Observability.DEBEZIUM_PAYLOAD_FLATTENER_PREFIX,
                    "get_date_partition",
                    Collections.emptyMap(),
                    () -> getDatePartitionInternal(now));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private DatePartition getDatePartitionInternal(Date now) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(now);
        return new DatePartition(
                calendar.get(Calendar.YEAR),
                calendar.get(Calendar.MONTH) + 1,
                calendar.get(Calendar.DAY_OF_MONTH));
    }

    public Dataset<Row> addDatePartitionColumns(Dataset<Row> df, Date now) {
        try {
            return observability.observeCallable(
                    Observability.DEBEZIUM_PAYLOAD_FLATTENER_PREFIX,
                    "add_date_partition_columns",
                    Collections.emptyMap(),
                    () -> addDatePartitionColumnsInternal(df, now));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Dataset<Row> addDatePartitionColumnsInternal(Dataset<Row> df, Date now) {
        DatePartition partition = getDatePartitionInternal(now);
        return df.withColumn("_year", lit(partition.year()))
                .withColumn("_month", lit(partition.month()))
                .withColumn("_day", lit(partition.day()));
    }

    public record DatePartition(int year, int month, int day) {
    }

}
