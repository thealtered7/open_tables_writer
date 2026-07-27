package com.thealtered7;

import com.thealtered7.datapipelines.KafkaWriteContext;
import com.thealtered7.models.TableUpdatedNotification.OpenTableFormat;
import java.nio.file.Path;

import org.apache.spark.sql.SparkSession;

public interface TableWriter {

    /**
     * Writes the given input file to the target table. Kafka coordinates and source identity are
     * used only for downstream write registration and may be {@code null} for one-shot invocations.
     */
    void writeToTable(
            SparkSession spark,
            Path inputFilePath,
            Path dataDirectoryBasePath,
            KafkaWriteContext kafka,
            SourceTableIdentity source);

    default void writeToTable(
            SparkSession spark, Path inputFilePath, Path dataDirectoryBasePath, KafkaWriteContext kafka) {
        writeToTable(spark, inputFilePath, dataDirectoryBasePath, kafka, null);
    }

    default void writeToTable(SparkSession spark, Path inputFilePath, Path dataDirectoryBasePath) {
        writeToTable(spark, inputFilePath, dataDirectoryBasePath, null, null);
    }

    OpenTableFormat format();
}
