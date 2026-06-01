package com.thealtered7;

import java.nio.file.Path;

import org.apache.spark.sql.SparkSession;

public interface TableWriter {

    void writeToTable(SparkSession spark, Path inputFilePath, Path dataDirectoryBasePath);
}
