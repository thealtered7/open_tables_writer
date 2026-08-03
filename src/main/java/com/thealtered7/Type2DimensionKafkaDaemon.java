package com.thealtered7;

import com.thealtered7.datapipelines.DatapipelinesClient;
import com.thealtered7.datapipelines.DatapipelinesHttpClient;
import com.thealtered7.datapipelines.KafkaWriteContext;
import com.thealtered7.models.TableUpdatedNotification;
import com.thealtered7.observability.Observability;
import com.thealtered7.observability.ObservabilityFactory;
import com.thealtered7.schemaregistry.SchemaAwareKafka;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Type2DimensionKafkaDaemon {

    private static final Logger log = LoggerFactory.getLogger(Type2DimensionKafkaDaemon.class);
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(1000);
    private static final String LOCAL_CATALOG_WAREHOUSE = "spark.sql.catalog.local_catalog.warehouse";

    public void run() {
        Type2DimensionConfigLoader config = Type2DimensionConfigLoader.load();
        Path silverWarehouse = config.silverWarehousePath();
        silverWarehouse.toFile().mkdirs();

        ObservabilityFactory observabilityFactory = ObservabilityFactory.create();
        Observability observability = observabilityFactory.observability();
        Runtime.getRuntime()
                .addShutdownHook(new Thread(observabilityFactory::shutdown, "type2-observability-shutdown"));

        SparkSession spark = new SparkSessionFactory().createType2SparkSession(silverWarehouse);
        DatapipelinesClient datapipelinesClient = DatapipelinesHttpClient.create(
                config.datapipelinesBaseUrl(),
                config.datapipelinesCatalogName(),
                config.datapipelinesJwtEnabled(),
                observability);
        Type2DimensionTransformer transformer =
                new Type2DimensionTransformer(observability, datapipelinesClient);
        KafkaConsumer<String, TableUpdatedNotification> consumer = createConsumer(config);
        java.util.concurrent.atomic.AtomicBoolean running = new java.util.concurrent.atomic.AtomicBoolean(true);

        Thread shutdownHook = new Thread(() -> {
            running.set(false);
            consumer.wakeup();
        }, "type2-dimension-kafka-shutdown");
        Runtime.getRuntime().addShutdownHook(shutdownHook);

        log.info(
                "Starting Kafka consumer: topic={}, group={}, client={}, schemaRegistry={}",
                config.topic(),
                config.groupId(),
                config.clientId(),
                config.schemaRegistryConfig().backend());

        try {
            consumer.subscribe(Collections.singletonList(config.topic()));
            while (running.get()) {
                observability.observeOperationVoid(Observability.TYPE2_DIMENSION_KAFKA_PREFIX, "poll", () -> {
                    ConsumerRecords<String, TableUpdatedNotification> records = consumer.poll(POLL_TIMEOUT);
                    observability.lowCardinalityTag("record_count", String.valueOf(records.count()));
                    for (ConsumerRecord<String, TableUpdatedNotification> record : records) {
                        processRecord(observability, transformer, spark, silverWarehouse, consumer, record);
                    }
                    return records.isEmpty() ? "empty" : "success";
                });
            }
        } catch (org.apache.kafka.common.errors.WakeupException e) {
            if (running.get()) {
                throw e;
            }
            log.info("Consumer wakeup for shutdown");
        } finally {
            Runtime.getRuntime().removeShutdownHook(shutdownHook);
            consumer.close();
            spark.stop();
            observabilityFactory.shutdown();
            log.info("Kafka consumer and Spark session stopped");
        }
    }

    private static KafkaConsumer<String, TableUpdatedNotification> createConsumer(Type2DimensionConfigLoader config) {
        return SchemaAwareKafka.createConsumer(
                config.bootstrapServers(),
                config.groupId(),
                config.clientId(),
                config.schemaRegistryConfig(),
                TableUpdatedNotification.class);
    }

    private static void processRecord(
            Observability observability,
            Type2DimensionTransformer transformer,
            SparkSession spark,
            Path silverWarehouse,
            KafkaConsumer<String, TableUpdatedNotification> consumer,
            ConsumerRecord<String, TableUpdatedNotification> record) {
        try {
            observability.observeCallableVoid(
                    Observability.TYPE2_DIMENSION_KAFKA_PREFIX,
                    "process_record",
                    Map.of(
                            "topic", record.topic(),
                            "partition", String.valueOf(record.partition()),
                            "offset", String.valueOf(record.offset())),
                    () -> {
                        TableUpdatedNotification notification = record.value();
                        if (notification == null) {
                            log.warn(
                                    "Skipping null table-updated notification at topic={}, partition={}, offset={}",
                                    record.topic(),
                                    record.partition(),
                                    record.offset());
                            commitOffset(consumer, record);
                            return "null_value";
                        }
                        observability.lowCardinalityTag("table", notification.tableFqn());
                        log.info(
                                "Processing table-updated notification: table_fqn={}, format={}, table_path={}, extract_job_id={}",
                                notification.tableFqn(),
                                notification.format(),
                                notification.tablePath(),
                                notification.extractJobId());

                        Type2TableAccess access = tableAccessFor(notification, silverWarehouse, spark);
                        transformer.transform(
                                spark,
                                access,
                                KafkaWriteContext.fromRecord(record),
                                Type2WriteIdentity.fromNotification(notification),
                                Type1WriteIdentity.fromNotification(notification));
                        commitOffset(consumer, record);
                        return "success";
                    });
        } catch (Exception e) {
            log.error(
                    "Failed to process record at topic={}, partition={}, offset={}",
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    e);
        }
    }

    private static Type2TableAccess tableAccessFor(
            TableUpdatedNotification notification, Path silverWarehouse, SparkSession spark) {
        Path tablePath = Path.of(notification.tablePath());
        IcebergTableIdentity identity = IcebergTableIdentity.fromTablePath(tablePath);
        spark.conf().set(LOCAL_CATALOG_WAREHOUSE, identity.getWarehouse().toAbsolutePath().toString());
        return new IcebergType2TableAccess(identity, silverWarehouse);
    }

    private static void commitOffset(
            KafkaConsumer<String, TableUpdatedNotification> consumer,
            ConsumerRecord<String, TableUpdatedNotification> record) {
        consumer.commitSync(Collections.singletonMap(
                new TopicPartition(record.topic(), record.partition()),
                new OffsetAndMetadata(record.offset() + 1)));
        log.info("Committed offset {} for partition {}", record.offset() + 1, record.partition());
    }
}
