package com.thealtered7;

import com.thealtered7.datapipelines.DatapipelinesClient;
import com.thealtered7.datapipelines.DatapipelinesHttpClient;
import com.thealtered7.datapipelines.KafkaWriteContext;
import com.thealtered7.datapipelines.TableWriteRegistration;
import com.thealtered7.models.FileFlushNotification;
import com.thealtered7.models.TableUpdatedNotification;
import com.thealtered7.observability.Observability;
import com.thealtered7.observability.ObservabilityFactory;
import com.thealtered7.schemaregistry.SchemaAwareKafka;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableWriterKafkaDaemon {

    private static final Logger log = LoggerFactory.getLogger(TableWriterKafkaDaemon.class);
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(1000);

    private final BiFunction<Observability, DatapipelinesClient, ? extends TableWriter> writerFactory;
    private final java.util.function.Function<Path, SparkSession> sparkSessionFactory;

    public TableWriterKafkaDaemon(
            BiFunction<Observability, DatapipelinesClient, ? extends TableWriter> writerFactory,
            java.util.function.Function<Path, SparkSession> sparkSessionFactory) {
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

        DatapipelinesClient datapipelinesClient = DatapipelinesHttpClient.create(
                config.datapipelinesBaseUrl(),
                config.datapipelinesCatalogName(),
                config.datapipelinesJwtEnabled(),
                observability);
        TableWriter writer = writerFactory.apply(observability, datapipelinesClient);
        SparkSession spark = sparkSessionFactory.apply(dataDirectoryBasePath);
        KafkaConsumer<String, FileFlushNotification> consumer = createConsumer(config);
        TableUpdatedNotificationPublisher publisher = new TableUpdatedNotificationPublisher(
                config.bootstrapServers(),
                config.clientId(),
                config.writeNotificationsTopic(),
                config.schemaRegistryConfig());
        AtomicBoolean running = new AtomicBoolean(true);

        Thread shutdownHook = new Thread(() -> {
            running.set(false);
            consumer.wakeup();
        }, "table-writer-kafka-shutdown");
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
                observability.observeOperationVoid(Observability.PREFIX, "poll", () -> {
                    ConsumerRecords<String, FileFlushNotification> records = consumer.poll(POLL_TIMEOUT);
                    observability.lowCardinalityTag("record_count", String.valueOf(records.count()));
                    for (ConsumerRecord<String, FileFlushNotification> record : records) {
                        processRecord(
                                observability,
                                config,
                                writer,
                                spark,
                                dataDirectoryBasePath,
                                consumer,
                                publisher,
                                record);
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
            publisher.close();
            spark.stop();
            observabilityFactory.shutdown();
            log.info("Kafka consumer and Spark session stopped");
        }
    }

    private static KafkaConsumer<String, FileFlushNotification> createConsumer(WriterConfigLoader config) {
        return SchemaAwareKafka.createConsumer(
                config.bootstrapServers(),
                config.groupId(),
                config.clientId(),
                config.schemaRegistryConfig(),
                FileFlushNotification.class);
    }

    private static void processRecord(
            Observability observability,
            WriterConfigLoader config,
            TableWriter writer,
            SparkSession spark,
            Path dataDirectoryBasePath,
            KafkaConsumer<String, FileFlushNotification> consumer,
            TableUpdatedNotificationPublisher publisher,
            ConsumerRecord<String, FileFlushNotification> record) {
        try {
            observability.observeCallableVoid(
                    Observability.PREFIX,
                    "process_record",
                    Map.of(
                            "topic", record.topic(),
                            "partition", String.valueOf(record.partition()),
                            "offset", String.valueOf(record.offset())),
                    () -> {
                        FileFlushNotification notification = record.value();
                        if (notification == null) {
                            log.warn(
                                    "Skipping null flush notification at topic={}, partition={}, offset={}",
                                    record.topic(),
                                    record.partition(),
                                    record.offset());
                            commitOffset(consumer, record);
                            return "null_value";
                        }
                        observability.lowCardinalityTag("table", notification.tableName());
                        log.info(
                                "Processing flush notification: rawFilePath={}, tableName={}, extractJobId={}, extractEndAt={}",
                                notification.rawFilePath(),
                                notification.tableName(),
                                notification.extractJobId(),
                                notification.extractEndAt());

                        Path inputFilePath = Path.of(notification.rawFilePath());
                        if (!InputFileWaiter.waitForFile(
                                inputFilePath, config.inputFileWaitMax(), config.inputFileWaitPollInterval())) {
                            log.error(
                                    "Input file not found after wait; skipping message. rawFilePath={}. "
                                            + "Ensure pgoutput and writer share host /opt/data "
                                            + "(pgoutput writes to /opt/data/raw).",
                                    inputFilePath);
                            commitOffset(consumer, record);
                            return "file_not_found";
                        }

                        SourceTableIdentity source = SourceTableIdentity.fromFlush(notification);
                        writer.writeToTable(
                                spark,
                                inputFilePath,
                                dataDirectoryBasePath,
                                KafkaWriteContext.fromRecord(record),
                                source,
                                notification);
                        publishTableUpdated(
                                publisher,
                                writer,
                                config,
                                notification,
                                source,
                                inputFilePath,
                                dataDirectoryBasePath);
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

    private static void publishTableUpdated(
            TableUpdatedNotificationPublisher publisher,
            TableWriter writer,
            WriterConfigLoader config,
            FileFlushNotification source,
            SourceTableIdentity sourceIdentity,
            Path inputFilePath,
            Path dataDirectoryBasePath) {
        String tableFqn = OpenTableNamespaces.toBronzeTableFqn(
                CdcInputFileNames.tableFqnFromFileName(inputFilePath.getFileName().toString()));
        Path tablePath = new DebeziumPayloadFlattener().getOutputTablePath(tableFqn, dataDirectoryBasePath);
        String databaseName = sourceIdentity != null ? sourceIdentity.databaseName() : null;
        String bronzeNamespace =
                sourceIdentity != null ? OpenTableNamespaces.bronze(sourceIdentity.schemaName()) : null;
        String tableName = sourceIdentity != null ? sourceIdentity.tableName() : null;
        TableUpdatedNotification notification = new TableUpdatedNotification(
                TableWriteRegistration.WRITE_TYPE_BRONZE,
                tableFqn,
                tablePath.toAbsolutePath().toString(),
                writer.format(),
                config.datapipelinesCatalogName(),
                databaseName,
                bronzeNamespace,
                tableName,
                dataDirectoryBasePath.toAbsolutePath().toString(),
                sourceIdentity != null ? sourceIdentity.instanceName() : null,
                databaseName,
                sourceIdentity != null ? sourceIdentity.schemaName() : null,
                tableName,
                source.rawFilePath(),
                source.rawFileSize(),
                source.extractJobId(),
                source.extractBufferId(),
                source.extractType(),
                source.extractStartAt(),
                source.extractEndAt(),
                source.keySchema(),
                source.valueSchema(),
                source.keySchemaId(),
                source.valueSchemaId());
        publisher.publish(notification);
    }

    private static void commitOffset(
            KafkaConsumer<String, FileFlushNotification> consumer,
            ConsumerRecord<String, FileFlushNotification> record) {
        consumer.commitSync(Collections.singletonMap(
                new TopicPartition(record.topic(), record.partition()),
                new OffsetAndMetadata(record.offset() + 1)));
        log.info("Committed offset {} for partition {}", record.offset() + 1, record.partition());
    }
}
