package com.thealtered7;

/**
 * Lakehouse open-table naming: bronze tables live in {@code ${source_schema}_bronze}, silver Type 1
 * tables in {@code ${source_schema}_silver.${source_table}_type1}, and silver Type 2 tables in
 * {@code ${source_schema}_silver.${source_table}_type2}.
 */
public final class OpenTableNamespaces {

    private static final String BRONZE_SUFFIX = "_bronze";
    private static final String SILVER_SUFFIX = "_silver";
    private static final String TYPE1_SUFFIX = "_type1";
    private static final String TYPE2_SUFFIX = "_type2";

    private OpenTableNamespaces() {}

    public static String bronze(String sourceSchemaName) {
        return sourceSchemaName + BRONZE_SUFFIX;
    }

    public static String silver(String sourceSchemaName) {
        return sourceSchemaName + SILVER_SUFFIX;
    }

    /** Silver Type 1 table name: {@code ${source_table}_type1}. */
    public static String type1Table(String sourceTableName) {
        return sourceTableName + TYPE1_SUFFIX;
    }

    /** Silver Type 2 table name: {@code ${source_table}_type2}. */
    public static String type2Table(String sourceTableName) {
        return sourceTableName + TYPE2_SUFFIX;
    }

    /** Maps a bronze open-table namespace ({@code public_bronze}) to silver ({@code public_silver}). */
    public static String silverFromBronze(String bronzeNamespace) {
        if (bronzeNamespace != null && bronzeNamespace.endsWith(BRONZE_SUFFIX)) {
            return bronzeNamespace.substring(0, bronzeNamespace.length() - BRONZE_SUFFIX.length())
                    + SILVER_SUFFIX;
        }
        return bronzeNamespace;
    }

    /**
     * Rewrites {@code database.schema.table} (OLTP / CDC file FQN) to the bronze open-table FQN
     * {@code database.schema_bronze.table}.
     */
    public static String toBronzeTableFqn(String sourceTableFqn) {
        String[] parts = sourceTableFqn.split("\\.", 3);
        if (parts.length != 3) {
            throw new IllegalArgumentException(
                    "Expected database.schema.table FQN, got: " + sourceTableFqn);
        }
        return parts[0] + "." + bronze(parts[1]) + "." + parts[2];
    }
}
