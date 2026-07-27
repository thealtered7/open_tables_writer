package com.thealtered7;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.thealtered7.datapipelines.DatapipelinesClient;
import com.thealtered7.datapipelines.DatapipelinesHttpClient;
import com.thealtered7.datapipelines.KafkaWriteContext;
import com.thealtered7.models.TableUpdatedNotification;
import com.thealtered7.models.TableUpdatedNotificationJson;
import com.thealtered7.observability.Observability;
import com.thealtered7.observability.ObservabilityFactory;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Type2DimensionKafkaDaemon {

    private static final Logger log = LoggerFactory.getLogger(Type2DimensionKafkaDaemon.class);
    private static final ObjectMapper OBJECT_MAPPER = TableUpdatedNotificationJson.MAPPER;
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
        KafkaConsumer<String, String> consumer = createConsumer(config);
        java.util.concurrent.atomic.AtomicBoolean running = new java.util.concurrent.atomic.AtomicBoolean(true);

        Thread shutdownHook = new Thread(() -> {
            running.set(false);
            consumer.wakeup();
        }, "type2-dimension-kafka-shutdown");
        Runtime.getRuntime().addShutdownHook(shutdownHook);

        log.info(
                "Starting Kafka consumer: topic={}, group={}, client={}",
                config.topic(),
                config.groupId(),
                config.clientId());

        try {
            consumer.subscribe(Collections.singletonList(config.topic()));
            while (running.get()) {
                observability.observeOperationVoid(Observability.TYPE2_DIMENSION_KAFKA_PREFIX, "poll", () -> {
                    ConsumerRecords<String, String> records = consumer.poll(POLL_TIMEOUT);
                    observability.lowCardinalityTag("record_count", String.valueOf(records.count()));
                    for (ConsumerRecord<String, String> record : records) {
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

    private static KafkaConsumer<String, String> createConsumer(Type2DimensionConfigLoader config) {
        var consumerProps = new java.util.Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, config.groupId());
        consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, config.clientId());
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return new KafkaConsumer<>(consumerProps);
    }

    private static void processRecord(
            Observability observability,
            Type2DimensionTransformer transformer,
            SparkSession spark,
            Path silverWarehouse,
            KafkaConsumer<String, String> consumer,
            ConsumerRecord<String, String> record) {
        try {
            observability.observeCallableVoid(
                    Observability.TYPE2_DIMENSION_KAFKA_PREFIX,
                    "process_record",
                    Map.of(
                            "topic", record.topic(),
                            "partition", String.valueOf(record.partition()),
                            "offset", String.valueOf(record.offset())),
                    () -> {
                        TableUpdatedNotification notification =
                                OBJECT_MAPPER.readValue(record.value(), TableUpdatedNotification.class);
                        observability.lowCardinalityTag("table", notification.tableFqn());
                        log.info(
                                "Processing table-updated notification: tableFqn={}, format={}, tablePath={}, runGuid={}",
                                notification.tableFqn(),
                                notification.format(),
                                notification.tablePath(),
                                notification.runGuid());

                        Type2TableAccess access = tableAccessFor(notification, silverWarehouse, spark);
                        transformer.transform(
                                spark,
                                access,
                                KafkaWriteContext.fromRecord(record),
                                Type2WriteIdentity.fromNotification(notification));
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

    private static void commitOffset(KafkaConsumer<String, String> consumer, ConsumerRecord<String, String> record) {
        consumer.commitSync(Collections.singletonMap(
                new TopicPartition(record.topic(), record.partition()),
                new OffsetAndMetadata(record.offset() + 1)));
        log.info("Committed offset {} for partition {}", record.offset() + 1, record.partition());
    }
}
