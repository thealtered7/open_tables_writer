package com.thealtered7.schema;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;

import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Evolves an Iceberg table to match Connect + DataFrame schemas and aligns the inbound DataFrame
 * for INSERT/MERGE (add nulls for soft-kept columns; cast when safe).
 */
public final class IcebergSchemaEvolver {

    private static final Logger log = LoggerFactory.getLogger(IcebergSchemaEvolver.class);

    public enum LayerMode {
        /** Business columns become {@code before_*} / {@code after_*}. */
        BRONZE,
        /** Business columns keep unprefixed names. */
        SILVER
    }

    private IcebergSchemaEvolver() {}

    /**
     * When the table does not exist yet, only aligns the DataFrame (create-as-select uses DF schema).
     * When it exists, applies ADD / DROP NOT NULL / safe type widenings then aligns the DF.
     */
    public static Dataset<Row> evolveAndAlign(
            SparkSession spark,
            String catalogTableName,
            String sqlTableName,
            Dataset<Row> incoming,
            String connectValueSchemaJson,
            LayerMode mode) {
        Map<String, ConnectField> businessFields = ConnectSchemaSupport.businessFields(connectValueSchemaJson);
        Map<String, DesiredColumn> desired = desiredColumns(incoming, businessFields, mode);

        if (!spark.catalog().tableExists(catalogTableName)) {
            return alignDataFrame(incoming, desired, null);
        }

        spark.catalog().refreshTable(catalogTableName);
        StructType tableSchema = spark.table(catalogTableName).schema();
        Map<String, StructField> tableFields = new LinkedHashMap<>();
        for (StructField field : tableSchema.fields()) {
            tableFields.put(field.name(), field);
        }

        for (DesiredColumn column : desired.values()) {
            StructField existing = tableFields.get(column.name());
            if (existing == null) {
                addColumn(spark, sqlTableName, column);
                continue;
            }
            maybeWidenOrFail(spark, sqlTableName, catalogTableName, existing, column);
        }

        for (StructField existing : tableFields.values()) {
            if (desired.containsKey(existing.name())) {
                continue;
            }
            // Soft-keep: never drop. Relax nullability only when needed.
            if (!existing.nullable()) {
                log.info("Relaxing NOT NULL on soft-kept column {}.{}", catalogTableName, existing.name());
                spark.sql(String.format(
                        "ALTER TABLE %s ALTER COLUMN `%s` DROP NOT NULL", sqlTableName, existing.name()));
            }
        }

        spark.catalog().refreshTable(catalogTableName);
        StructType evolvedSchema = spark.table(catalogTableName).schema();
        return alignDataFrame(incoming, desired, evolvedSchema);
    }

    static Map<String, DesiredColumn> desiredColumns(
            Dataset<Row> incoming, Map<String, ConnectField> businessFields, LayerMode mode) {
        Map<String, DesiredColumn> desired = new LinkedHashMap<>();

        for (StructField field : incoming.schema().fields()) {
            desired.put(
                    field.name(),
                    new DesiredColumn(field.name(), field.dataType(), sqlType(field.dataType()), null));
        }

        for (ConnectField business : businessFields.values()) {
            if (mode == LayerMode.BRONZE) {
                putBusiness(desired, "before_" + business.name(), business);
                putBusiness(desired, "after_" + business.name(), business);
            } else {
                putBusiness(desired, business.name(), business);
            }
        }
        return desired;
    }

    private static void putBusiness(Map<String, DesiredColumn> desired, String name, ConnectField business) {
        DesiredColumn fromConnect = new DesiredColumn(
                name, business.dataType(), business.sqlType(), business.hasDefault() ? business.defaultLiteral() : null);
        DesiredColumn existing = desired.get(name);
        if (existing == null) {
            desired.put(name, fromConnect);
            return;
        }
        String defaultLiteral =
                fromConnect.defaultLiteral() != null ? fromConnect.defaultLiteral() : existing.defaultLiteral();
        // Post-convert DF timestamps win over Connect string (Debezium often declares ZonedTimestamp as string).
        if (isTimestampFamily(existing.dataType()) && business.dataType().sameType(DataTypes.StringType)) {
            desired.put(
                    name,
                    new DesiredColumn(name, existing.dataType(), existing.sqlType(), defaultLiteral));
            return;
        }
        // Prefer Connect type/default when DF already has the column (Connect is source of intent).
        desired.put(
                name,
                new DesiredColumn(name, business.dataType(), business.sqlType(), defaultLiteral));
    }

    private static void addColumn(SparkSession spark, String sqlTableName, DesiredColumn column) {
        log.info("Adding column {}.{} {}", sqlTableName, column.name(), column.sqlType());
        spark.sql(String.format(
                "ALTER TABLE %s ADD COLUMN `%s` %s", sqlTableName, column.name(), column.sqlType()));
        if (column.defaultLiteral() != null) {
            spark.sql(String.format(
                    "UPDATE %s SET `%s` = %s WHERE `%s` IS NULL",
                    sqlTableName, column.name(), column.defaultLiteral(), column.name()));
        }
    }

    private static void maybeWidenOrFail(
            SparkSession spark,
            String sqlTableName,
            String catalogTableName,
            StructField existing,
            DesiredColumn desired) {
        DataType from = existing.dataType();
        DataType to = desired.dataType();
        if (compatibleSame(from, to)) {
            return;
        }
        if (isSafeCoercion(from, to)) {
            log.info(
                    "Widening column {}.{} from {} to {}",
                    catalogTableName,
                    existing.name(),
                    from.simpleString(),
                    to.simpleString());
            spark.sql(String.format(
                    "ALTER TABLE %s ALTER COLUMN `%s` TYPE %s",
                    sqlTableName, existing.name(), desired.sqlType()));
            return;
        }
        if (isSafeCoercion(to, from)) {
            // Incoming is narrower; keep table type and cast DF later.
            return;
        }
        throw new SchemaIncompatibleException(
                catalogTableName, existing.name(), from.simpleString(), to.simpleString());
    }

    static boolean compatibleSame(DataType left, DataType right) {
        if (left.sameType(right)) {
            return true;
        }
        // Treat timestamp flavors as equivalent for equality.
        return isTimestampFamily(left) && isTimestampFamily(right);
    }

    static boolean isSafeCoercion(DataType from, DataType to) {
        if (compatibleSame(from, to)) {
            return true;
        }
        if (isTimestampFamily(from) && isTimestampFamily(to)) {
            return true;
        }
        // ISO timestamp strings can widen to timestamp (bronze convertTimestampColumns).
        // Never treat timestamp → string as a table widen (that would destroy typed columns).
        if (from.sameType(DataTypes.StringType) && isTimestampFamily(to)) {
            return true;
        }
        // String length is not enforced in Spark StringType; treat as no-op compatible.
        if (from.sameType(DataTypes.StringType) && to.sameType(DataTypes.StringType)) {
            return true;
        }
        if (from instanceof DecimalType fromDecimal && to instanceof DecimalType toDecimal) {
            return toDecimal.precision() >= fromDecimal.precision() && toDecimal.scale() >= fromDecimal.scale();
        }
        // Integer widenings commonly seen with JDBC/JSON inference.
        if (from.sameType(DataTypes.IntegerType) && to.sameType(DataTypes.LongType)) {
            return true;
        }
        if (from.sameType(DataTypes.FloatType) && to.sameType(DataTypes.DoubleType)) {
            return true;
        }
        return false;
    }

    static boolean isTimestampFamily(DataType type) {
        String name = type.typeName();
        return "timestamp".equals(name) || "timestamp_ntz".equals(name);
    }

    static Dataset<Row> alignDataFrame(
            Dataset<Row> incoming, Map<String, DesiredColumn> desired, StructType tableSchema) {
        Dataset<Row> result = incoming;
        Set<String> incomingNames = new HashSet<>(Arrays.asList(incoming.columns()));

        // Ensure Connect-declared columns exist on the DF, typed to Connect/desired (not JSON inference).
        for (DesiredColumn column : desired.values()) {
            if (!incomingNames.contains(column.name())) {
                result = result.withColumn(column.name(), lit(null).cast(column.dataType()));
                incomingNames.add(column.name());
            } else {
                StructField incomingField = result.schema().apply(column.name());
                if (!incomingField.dataType().sameType(column.dataType())) {
                    result = result.withColumn(column.name(), col(column.name()).cast(column.dataType()));
                }
            }
        }

        if (tableSchema == null) {
            return result;
        }

        // Soft-kept table columns missing from inbound DF → null; otherwise always cast to table type
        // so JSON null-inference (e.g. before_* as string) cannot fail INSERT into typed Iceberg columns.
        for (StructField tableField : tableSchema.fields()) {
            if (!incomingNames.contains(tableField.name())) {
                result = result.withColumn(tableField.name(), lit(null).cast(tableField.dataType()));
                incomingNames.add(tableField.name());
            } else {
                StructField incomingField = result.schema().apply(tableField.name());
                if (!incomingField.dataType().sameType(tableField.dataType())) {
                    result = result.withColumn(tableField.name(), col(tableField.name()).cast(tableField.dataType()));
                }
            }
        }

        // Order columns as table expects for INSERT * / MERGE INSERT *.
        Set<String> ordered = new LinkedHashSet<>();
        for (StructField field : tableSchema.fields()) {
            ordered.add(field.name());
        }
        for (String name : result.columns()) {
            ordered.add(name);
        }
        return result.select(ordered.stream().map(org.apache.spark.sql.functions::col).toArray(org.apache.spark.sql.Column[]::new));
    }

    private static String sqlType(DataType dataType) {
        return ConnectSchemaSupport.toSqlType(dataType);
    }

    record DesiredColumn(String name, DataType dataType, String sqlType, String defaultLiteral) {}
}
