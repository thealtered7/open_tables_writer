package com.thealtered7;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Set;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.to_timestamp;

public final class DebeziumPayloadFlattener {

    private static final Set<String> PREFIXED_STRUCTS = Set.of("before", "after", "source");
    private static final Set<String> TOP_LEVEL_FIELDS = Set.of("op", "transaction", "ts_ms", "ts_ns", "ts_us");
    private static final String ISO_TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss[.SSSSSS]'Z'";

    public DebeziumPayloadFlattener() {
    }

    public Dataset<Row> loadJsonLines(SparkSession spark, Path inputFilePath) {
        return spark.read().json(inputFilePath.toString());
    }

    public Dataset<Row> flattenPayload(Dataset<Row> raw) {
        StructType payloadSchema = (StructType) raw.schema().apply("payload").dataType();

        List<Column> columns = new ArrayList<>();
        for (StructField field : payloadSchema.fields()) {
            String name = field.name();
            if (PREFIXED_STRUCTS.contains(name)) {
                expandStruct(columns, name, (StructType) field.dataType());
            } else if (TOP_LEVEL_FIELDS.contains(name)) {
                columns.add(col("payload." + name).alias(name));
            }
        }

        return raw.select(columns.toArray(Column[]::new));
    }

    private void expandStruct(List<Column> columns, String structName, StructType structType) {
        for (StructField nested : structType.fields()) {
            String alias = structName + "_" + nested.name();
            columns.add(col("payload." + structName + "." + nested.name()).alias(alias));
        }
    }

    public Dataset<Row> convertTimestampColumns(Dataset<Row> flat) {
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
        return name.endsWith("_created_at") || name.endsWith("_updated_at");
    }

    public Path getOutputTablePath(String tableFQN, Path dataDirectoryBasePath) {
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
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(now);
        return new DatePartition(
                calendar.get(Calendar.YEAR),
                calendar.get(Calendar.MONTH) + 1,
                calendar.get(Calendar.DAY_OF_MONTH));
    }

    public Dataset<Row> addDatePartitionColumns(Dataset<Row> df, Date now) {
        DatePartition partition = getDatePartition(now);
        return df.withColumn("year", lit(partition.year()))
                .withColumn("month", lit(partition.month()))
                .withColumn("day", lit(partition.day()));
    }

    public record DatePartition(int year, int month, int day) {
    }

}
