package com.thealtered7;

import java.nio.file.Path;
import java.util.Objects;
import org.apache.spark.sql.SparkSession;

/** One-shot entry point for local file testing via Makefile. */
public final class DeltaTableWriterOneShot {

    private DeltaTableWriterOneShot() {}

    public static void main(String[] args) {
        String inputFile = System.getProperty("input.file.path");
        String dataDirectoryBase = System.getProperty("data.directory.base.path");
        Objects.requireNonNull(inputFile, "input.file.path is required");
        Objects.requireNonNull(dataDirectoryBase, "data.directory.base.path is required");
        Path inputFilePath = Path.of(inputFile);
        Path dataDirectoryBasePath = Path.of(dataDirectoryBase);
        dataDirectoryBasePath.toFile().mkdirs();

        SparkSession spark = new SparkSessionFactory().createDeltaTableSparkSession(dataDirectoryBasePath);
        try {
            new DeltaTableWriter().writeToTable(spark, inputFilePath, dataDirectoryBasePath);
        } finally {
            spark.stop();
        }
    }
}
