package com.thealtered7;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.thealtered7.models.FileFlushNotification;
import com.thealtered7.models.FileFlushNotificationJson;
import com.thealtered7.observability.Observability;
import com.thealtered7.observability.ObservabilityFactory;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
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

public class TableWriterKafkaDaemon {

    private static final Logger log = LoggerFactory.getLogger(TableWriterKafkaDaemon.class);
    private static final ObjectMapper OBJECT_MAPPER = FileFlushNotificationJson.MAPPER;
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(1000);

    private final Function<Observability, ? extends TableWriter> writerFactory;
    private final Function<Path, SparkSession> sparkSessionFactory;

    public TableWriterKafkaDaemon(
            Function<Observability, ? extends TableWriter> writerFactory,
            Function<Path, SparkSession> sparkSessionFactory) {
        this.writerFactory = Objects.requireNonNull(writerFactory, "writerFactory");
        this.sparkSessionFactory = Objects.requireNonNull(sparkSessionFactory, "sparkSessionFactory");
    }

    public void run() {
        WriterConfigLoader config = WriterConfigLoader.load();
        Path dataDirectoryBasePath = Path.of(config.dataDirectoryBasePath());
        dataDirectoryBasePath.toFile().mkdirs();

        ObservabilityFactory observabilityFactory = ObservabilityFactory.create();
        Observability observability = observabilityFactory.observability();
        Runtime.getRuntime().addShutdownHook(new Thread(observabilityFactory::shutdown, "observability-shutdown"));

        TableWriter writer = writerFactory.apply(observability);
        SparkSession spark = sparkSessionFactory.apply(dataDirectoryBasePath);
        KafkaConsumer<String, String> consumer = createConsumer(config);
        AtomicBoolean running = new AtomicBoolean(true);

        Thread shutdownHook = new Thread(() -> {
            running.set(false);
            consumer.wakeup();
        }, "table-writer-kafka-shutdown");
        Runtime.getRuntime().addShutdownHook(shutdownHook);

        log.info(
                "Starting Kafka consumer: topic={}, group={}, client={}",
                config.topic(),
                config.groupId(),
                config.clientId());

        try {
            consumer.subscribe(Collections.singletonList(config.topic()));
            while (running.get()) {
                observability.observeOperationVoid(Observability.PREFIX, "poll", () -> {
                    ConsumerRecords<String, String> records = consumer.poll(POLL_TIMEOUT);
                    observability.lowCardinalityTag("record_count", String.valueOf(records.count()));
                    for (ConsumerRecord<String, String> record : records) {
                        processRecord(observability, config, writer, spark, dataDirectoryBasePath, consumer, record);
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

    private static KafkaConsumer<String, String> createConsumer(WriterConfigLoader config) {
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
            WriterConfigLoader config,
            TableWriter writer,
            SparkSession spark,
            Path dataDirectoryBasePath,
            KafkaConsumer<String, String> consumer,
            ConsumerRecord<String, String> record) {
        try {
            observability.observeCallableVoid(
                    Observability.PREFIX,
                    "process_record",
                    Map.of(
                            "topic", record.topic(),
                            "partition", String.valueOf(record.partition()),
                            "offset", String.valueOf(record.offset())),
                    () -> {
                        FileFlushNotification notification =
                                OBJECT_MAPPER.readValue(record.value(), FileFlushNotification.class);
                        observability.lowCardinalityTag("table", notification.tableName());
                        log.info(
                                "Processing flush notification: filePath={}, tableName={}, runGuid={}, writtenAt={}",
                                notification.filePath(),
                                notification.tableName(),
                                notification.runGuid(),
                                notification.writtenAt());

                        Path inputFilePath = Path.of(notification.filePath());
                        if (!InputFileWaiter.waitForFile(
                                inputFilePath, config.inputFileWaitMax(), config.inputFileWaitPollInterval())) {
                            log.error(
                                    "Input file not found after wait; skipping message. filePath={}. "
                                            + "Ensure pgoutput and writer share host /opt/data "
                                            + "(pgoutput writes to /opt/data/raw).",
                                    inputFilePath);
                            commitOffset(consumer, record);
                            return "file_not_found";
                        }

                        writer.writeToTable(spark, inputFilePath, dataDirectoryBasePath);
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

    private static void commitOffset(KafkaConsumer<String, String> consumer, ConsumerRecord<String, String> record) {
        consumer.commitSync(Collections.singletonMap(
                new TopicPartition(record.topic(), record.partition()),
                new OffsetAndMetadata(record.offset() + 1)));
        log.info("Committed offset {} for partition {}", record.offset() + 1, record.partition());
    }
}
