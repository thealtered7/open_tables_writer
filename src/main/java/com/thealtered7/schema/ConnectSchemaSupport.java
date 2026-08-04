package com.thealtered7.schema;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Parses Debezium Kafka Connect JSON schema documents and extracts business row fields from the
 * {@code after} (preferred) or {@code before} struct.
 */
public final class ConnectSchemaSupport {

    private static final Logger log = LoggerFactory.getLogger(ConnectSchemaSupport.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private ConnectSchemaSupport() {}

    /**
     * @param connectValueSchemaJson Connect schema object JSON (not a full envelope), or null/blank
     * @return field name → definition; empty when unparseable or absent
     */
    public static Map<String, ConnectField> businessFields(String connectValueSchemaJson) {
        if (connectValueSchemaJson == null || connectValueSchemaJson.isBlank()) {
            return Collections.emptyMap();
        }
        try {
            JsonNode root = MAPPER.readTree(connectValueSchemaJson);
            JsonNode rowStruct = resolveRowStruct(root);
            if (rowStruct == null || !rowStruct.has("fields") || !rowStruct.get("fields").isArray()) {
                return Collections.emptyMap();
            }
            Map<String, ConnectField> fields = new LinkedHashMap<>();
            for (JsonNode fieldNode : rowStruct.get("fields")) {
                ConnectField field = parseField(fieldNode);
                if (field != null) {
                    fields.put(field.name(), field);
                }
            }
            return fields;
        } catch (IOException e) {
            log.warn("Failed to parse Connect value schema; treating as empty", e);
            return Collections.emptyMap();
        }
    }

    static JsonNode resolveRowStruct(JsonNode root) {
        if (root == null || root.isNull()) {
            return null;
        }
        JsonNode fields = root.get("fields");
        if (fields != null && fields.isArray()) {
            JsonNode after = findNamedStruct(fields, "after");
            if (after != null) {
                return after;
            }
            JsonNode before = findNamedStruct(fields, "before");
            if (before != null) {
                return before;
            }
            // Already a row-level struct (no envelope wrappers).
            return root;
        }
        return root;
    }

    private static JsonNode findNamedStruct(JsonNode fields, String name) {
        for (JsonNode field : fields) {
            if (name.equals(text(field, "field")) && "struct".equals(text(field, "type"))) {
                return field;
            }
        }
        return null;
    }

    static ConnectField parseField(JsonNode fieldNode) {
        String name = text(fieldNode, "field");
        if (name == null || name.isBlank()) {
            return null;
        }
        String type = text(fieldNode, "type");
        String nameAnnotation = text(fieldNode, "name");
        JsonNode parameters = fieldNode.get("parameters");
        DataType dataType = toSparkType(type, nameAnnotation, parameters);
        String sqlType = toSqlType(dataType);
        boolean optional = fieldNode.path("optional").asBoolean(true);
        String defaultLiteral = defaultLiteral(fieldNode.get("default"), dataType);
        return new ConnectField(name, dataType, sqlType, optional, defaultLiteral);
    }

    static DataType toSparkType(String type, String nameAnnotation, JsonNode parameters) {
        if (type == null) {
            return DataTypes.StringType;
        }
        return switch (type) {
            case "int8", "int16", "int32" -> DataTypes.IntegerType;
            case "int64" -> isTimestampLogical(nameAnnotation) ? DataTypes.TimestampType : DataTypes.LongType;
            case "float", "float32" -> DataTypes.FloatType;
            case "double", "float64" -> DataTypes.DoubleType;
            case "boolean" -> DataTypes.BooleanType;
            case "bytes" -> isDecimalLogical(nameAnnotation)
                    ? decimalType(parameters)
                    : DataTypes.BinaryType;
            case "string" -> {
                if (isTimestampLogical(nameAnnotation)) {
                    yield DataTypes.TimestampType;
                }
                if (isDateLogical(nameAnnotation)) {
                    yield DataTypes.DateType;
                }
                yield DataTypes.StringType;
            }
            default -> DataTypes.StringType;
        };
    }

    private static boolean isTimestampLogical(String nameAnnotation) {
        if (nameAnnotation == null) {
            return false;
        }
        return nameAnnotation.contains("Timestamp")
                || nameAnnotation.equals("org.apache.kafka.connect.data.Timestamp");
    }

    private static boolean isDateLogical(String nameAnnotation) {
        if (nameAnnotation == null) {
            return false;
        }
        return nameAnnotation.contains("Date") || nameAnnotation.equals("org.apache.kafka.connect.data.Date");
    }

    private static boolean isDecimalLogical(String nameAnnotation) {
        return nameAnnotation != null && nameAnnotation.contains("Decimal");
    }

    private static DecimalType decimalType(JsonNode parameters) {
        int precision = 38;
        int scale = 10;
        if (parameters != null) {
            if (parameters.has("connect.decimal.precision")) {
                precision = parameters.get("connect.decimal.precision").asInt(precision);
            }
            if (parameters.has("scale")) {
                scale = Integer.parseInt(parameters.get("scale").asText("10"));
            }
        }
        return DataTypes.createDecimalType(precision, scale);
    }

    static String toSqlType(DataType dataType) {
        if (dataType instanceof DecimalType decimalType) {
            return String.format("decimal(%d,%d)", decimalType.precision(), decimalType.scale());
        }
        return switch (dataType.typeName()) {
            case "integer" -> "int";
            case "long" -> "bigint";
            case "float" -> "float";
            case "double" -> "double";
            case "boolean" -> "boolean";
            case "binary" -> "binary";
            case "date" -> "date";
            case "timestamp" -> "timestamp";
            default -> "string";
        };
    }

    private static String defaultLiteral(JsonNode defaultNode, DataType dataType) {
        if (defaultNode == null || defaultNode.isNull() || defaultNode.isMissingNode()) {
            return null;
        }
        if (defaultNode.isBoolean() || defaultNode.isNumber()) {
            return defaultNode.asText();
        }
        if (defaultNode.isTextual()) {
            String text = defaultNode.asText();
            if (dataType != null && dataType.sameType(DataTypes.StringType)) {
                return "'" + text.replace("'", "''") + "'";
            }
            return text;
        }
        return null;
    }

    private static String text(JsonNode node, String field) {
        JsonNode value = node.get(field);
        return value == null || value.isNull() ? null : value.asText();
    }
}
