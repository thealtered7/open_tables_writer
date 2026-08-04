package com.thealtered7.schema;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class IcebergSchemaEvolverTest {

    private static SparkSession spark;

    @BeforeAll
    static void startSpark() {
        spark = SparkSession.builder()
                .master("local[1]")
                .appName("IcebergSchemaEvolverTest")
                .config("spark.ui.enabled", "false")
                .getOrCreate();
    }

    @AfterAll
    static void stopSpark() {
        if (spark != null) {
            spark.stop();
        }
    }

    @Test
    void isSafeCoercion_allowsDecimalWidenAndTimestampFlavor() {
        assertTrue(IcebergSchemaEvolver.isSafeCoercion(
                DataTypes.createDecimalType(10, 2), DataTypes.createDecimalType(18, 4)));
        assertTrue(IcebergSchemaEvolver.isSafeCoercion(DataTypes.StringType, DataTypes.StringType));
        assertTrue(IcebergSchemaEvolver.isSafeCoercion(DataTypes.IntegerType, DataTypes.LongType));
        assertTrue(IcebergSchemaEvolver.compatibleSame(DataTypes.TimestampType, DataTypes.TimestampType));
        assertFalse(IcebergSchemaEvolver.isSafeCoercion(DataTypes.IntegerType, DataTypes.StringType));
        assertFalse(IcebergSchemaEvolver.isSafeCoercion(
                DataTypes.createDecimalType(18, 4), DataTypes.createDecimalType(10, 2)));
    }

    @Test
    void isSafeCoercion_allowsStringToTimestampButNotTimestampToString() {
        assertTrue(IcebergSchemaEvolver.isSafeCoercion(DataTypes.StringType, DataTypes.TimestampType));
        assertFalse(IcebergSchemaEvolver.isSafeCoercion(DataTypes.TimestampType, DataTypes.StringType));
    }

    @Test
    void desiredColumns_keepsDfTimestampWhenConnectDeclaresString() {
        StructType dfSchema = new StructType()
                .add("after_created_at", DataTypes.TimestampType, true)
                .add("after_id", DataTypes.IntegerType, true);
        var df = spark.createDataFrame(
                java.util.List.of(RowFactory.create(null, 1)), dfSchema);

        String connectSchema =
                """
                {
                  "type":"struct",
                  "fields":[
                    {"field":"after","type":"struct","fields":[
                      {"field":"id","type":"int32"},
                      {"field":"created_at","type":"string"}
                    ]}
                  ]
                }
                """;
        Map<String, ConnectField> business = ConnectSchemaSupport.businessFields(connectSchema);
        Map<String, IcebergSchemaEvolver.DesiredColumn> desired = IcebergSchemaEvolver.desiredColumns(
                df, business, IcebergSchemaEvolver.LayerMode.BRONZE);

        assertEquals(DataTypes.TimestampType, desired.get("after_created_at").dataType());
        assertEquals(DataTypes.IntegerType, desired.get("after_id").dataType());
    }

    @Test
    void alignDataFrame_castsStringNullColumnToTableInt() {
        StructType dfSchema = new StructType()
                .add("before_fart_2", DataTypes.StringType, true)
                .add("after_fart_2", DataTypes.IntegerType, true)
                .add("after_id", DataTypes.IntegerType, true);
        var df = spark.createDataFrame(
                java.util.List.of(RowFactory.create(null, 7, 1)), dfSchema);

        StructType tableSchema = new StructType()
                .add("before_fart_2", DataTypes.IntegerType, true)
                .add("after_fart_2", DataTypes.IntegerType, true)
                .add("after_id", DataTypes.IntegerType, true);

        Map<String, IcebergSchemaEvolver.DesiredColumn> desired = Map.of(
                "before_fart_2",
                new IcebergSchemaEvolver.DesiredColumn(
                        "before_fart_2", DataTypes.IntegerType, "int", null),
                "after_fart_2",
                new IcebergSchemaEvolver.DesiredColumn(
                        "after_fart_2", DataTypes.IntegerType, "int", null),
                "after_id",
                new IcebergSchemaEvolver.DesiredColumn("after_id", DataTypes.IntegerType, "int", null));

        var aligned = IcebergSchemaEvolver.alignDataFrame(df, desired, tableSchema);

        assertEquals(DataTypes.IntegerType, aligned.schema().apply("before_fart_2").dataType());
        assertEquals(DataTypes.IntegerType, aligned.schema().apply("after_fart_2").dataType());
        Row row = aligned.collectAsList().get(0);
        assertEquals(null, row.get(0));
        assertEquals(7, row.getInt(1));
        assertEquals(1, row.getInt(2));
    }

    @Test
    void schemaIncompatibleException_includesColumnDetails() {
        SchemaIncompatibleException ex =
                assertThrows(SchemaIncompatibleException.class, () -> {
                    throw new SchemaIncompatibleException("db.ns.t", "amount", "int", "string");
                });
        assertTrue(ex.getMessage().contains("amount"));
        assertTrue(ex.getMessage().contains("int"));
        assertTrue(ex.getMessage().contains("string"));
    }
}
