package com.thealtered7;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.thealtered7.models.DeadLetterMessage;
import com.thealtered7.schemaregistry.SchemaAwareKafka;
import com.thealtered7.schemaregistry.SchemaRegistryConfig;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Publishes wrapped dead-letter messages. Callers commit the main-topic offset only after a
 * successful {@link #publish} returns.
 */
public final class DeadLetterPublisher implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterPublisher.class);
    private static final ObjectMapper MAPPER = new ObjectMapper().registerModule(new JavaTimeModule());

    private final Producer<String, DeadLetterMessage> producer;
    private final String dlqTopic;

    public DeadLetterPublisher(
            String bootstrapServers, String clientId, String dlqTopic, SchemaRegistryConfig schemaRegistryConfig) {
        this(
                SchemaAwareKafka.createProducer(
                        bootstrapServers,
                        clientId == null ? null : clientId + "-dlq",
                        schemaRegistryConfig,
                        DeadLetterMessage.class),
                dlqTopic);
    }

    DeadLetterPublisher(Producer<String, DeadLetterMessage> producer, String dlqTopic) {
        this.producer = Objects.requireNonNull(producer, "producer");
        this.dlqTopic = Objects.requireNonNull(dlqTopic, "dlqTopic");
        if (dlqTopic.isBlank()) {
            throw new IllegalArgumentException("dlqTopic must not be blank");
        }
    }

    public <T> void publish(
            ConsumerRecord<String, T> failedRecord,
            Exception error,
            String extractJobId,
            String extractBufferId,
            String tableIdentity) {
        DeadLetterMessage message = new DeadLetterMessage(
                failedRecord.topic(),
                failedRecord.partition(),
                failedRecord.offset(),
                Instant.now(),
                error.getClass().getName(),
                error.getMessage(),
                stackTrace(error),
                ExtractMdc.normalize(extractJobId),
                ExtractMdc.normalize(extractBufferId),
                tableIdentity,
                toJsonNode(failedRecord.value()));
        try {
            RecordMetadata metadata = producer
                    .send(new ProducerRecord<>(dlqTopic, failedRecord.key(), message))
                    .get();
            log.error(
                    "Published dead-letter message to topic={}, partition={}, offset={} for original {}:{}:{}",
                    metadata.topic(),
                    metadata.partition(),
                    metadata.offset(),
                    failedRecord.topic(),
                    failedRecord.partition(),
                    failedRecord.offset(),
                    error);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while publishing to DLQ topic " + dlqTopic, e);
        } catch (ExecutionException e) {
            throw new IllegalStateException("Failed to publish to DLQ topic " + dlqTopic, e.getCause());
        } catch (RuntimeException e) {
            throw new IllegalStateException("Failed to publish to DLQ topic " + dlqTopic, e);
        }
    }

    private static String stackTrace(Throwable error) {
        StringWriter writer = new StringWriter();
        error.printStackTrace(new PrintWriter(writer));
        return writer.toString();
    }

    private static JsonNode toJsonNode(Object value) {
        if (value == null) {
            return MAPPER.nullNode();
        }
        return MAPPER.valueToTree(value);
    }

    @Override
    public void close() {
        producer.close();
    }
}
