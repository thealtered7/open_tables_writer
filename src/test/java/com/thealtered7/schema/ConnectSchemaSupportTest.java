package com.thealtered7.schema;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;
import org.junit.jupiter.api.Test;

class ConnectSchemaSupportTest {

    @Test
    void businessFields_readsAfterStructAndDefault() {
        String schema =
                """
                {
                  "type":"struct",
                  "fields":[
                    {"field":"before","type":"struct","fields":[{"field":"id","type":"int32"}]},
                    {"field":"after","type":"struct","fields":[
                      {"field":"id","type":"int32"},
                      {"field":"status","type":"string","optional":true,"default":"active"}
                    ]},
                    {"field":"op","type":"string"}
                  ]
                }
                """;

        Map<String, ConnectField> fields = ConnectSchemaSupport.businessFields(schema);

        assertEquals(2, fields.size());
        assertEquals(DataTypes.IntegerType, fields.get("id").dataType());
        assertEquals("'active'", fields.get("status").defaultLiteral());
        assertTrue(fields.get("status").hasDefault());
        assertFalse(fields.get("id").hasDefault());
    }

    @Test
    void businessFields_parsesDecimalParameters() {
        String schema =
                """
                {
                  "type":"struct",
                  "fields":[
                    {"field":"after","type":"struct","fields":[
                      {"field":"amount","type":"bytes","name":"org.apache.kafka.connect.data.Decimal",
                       "parameters":{"scale":"2","connect.decimal.precision":"12"}}
                    ]}
                  ]
                }
                """;

        ConnectField amount = ConnectSchemaSupport.businessFields(schema).get("amount");
        assertTrue(amount.dataType() instanceof DecimalType);
        assertEquals("decimal(12,2)", amount.sqlType());
    }

    @Test
    void businessFields_mapsZonedTimestampStringToTimestamp() {
        String schema =
                """
                {
                  "type":"struct",
                  "fields":[
                    {"field":"after","type":"struct","fields":[
                      {"field":"created_at","type":"string","name":"io.debezium.time.ZonedTimestamp"}
                    ]}
                  ]
                }
                """;

        ConnectField createdAt = ConnectSchemaSupport.businessFields(schema).get("created_at");
        assertEquals(DataTypes.TimestampType, createdAt.dataType());
        assertEquals("timestamp", createdAt.sqlType());
    }

    @Test
    void businessFields_emptyWhenBlank() {
        assertTrue(ConnectSchemaSupport.businessFields(null).isEmpty());
        assertTrue(ConnectSchemaSupport.businessFields("").isEmpty());
    }
}
