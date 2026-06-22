package com.thealtered7;

import java.nio.file.Path;

import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkSessionFactory {
    private static final Logger log = LoggerFactory.getLogger(SparkSessionFactory.class);

    public SparkSessionFactory() {
    }

    public SparkSession createDeltaTableSparkSession(Path warehousePath) {
        log.info("creating delta table spark session with warehouse: {}", warehousePath);
        SparkSession spark = SparkSession.builder()
                .appName("Write to Delta Lake")
                .master("local[*]")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .config("spark.sql.warehouse.dir", warehousePath.toString())
                .config("spark.sql.shuffle.partitions", "4")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");
        return spark;
    }

    public SparkSession createIcebergTableSparkSession(Path warehousePath) {
        log.info("creating iceberg table spark session with warehouse: {}", warehousePath);
        SparkSession spark = SparkSession.builder()
                .appName("Write to Apache Iceberg")
                .master("local[*]")
                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                .config("spark.sql.catalog.local_catalog", "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog.local_catalog.type", "hadoop")
                .config("spark.sql.catalog.local_catalog.warehouse", warehousePath.toString())
                .config("spark.sql.shuffle.partitions", "4")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");
        return spark;
    }

    public SparkSession createIcebergTableSparkSession(Path sourceWarehouse, Path silverWarehouse) {
        log.info(
                "creating iceberg table spark session with source warehouse: {} and silver warehouse: {}",
                sourceWarehouse,
                silverWarehouse);
        SparkSession spark = SparkSession.builder()
                .appName("Write to Apache Iceberg")
                .master("local[*]")
                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                .config("spark.sql.catalog.local_catalog", "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog.local_catalog.type", "hadoop")
                .config("spark.sql.catalog.local_catalog.warehouse", sourceWarehouse.toString())
                .config("spark.sql.catalog.silver_catalog", "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog.silver_catalog.type", "hadoop")
                .config("spark.sql.catalog.silver_catalog.warehouse", silverWarehouse.toString())
                .config("spark.sql.shuffle.partitions", "4")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");
        return spark;
    }
}
