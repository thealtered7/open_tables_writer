package com.thealtered7;

import com.thealtered7.models.TableUpdatedNotification;
import com.thealtered7.schemaregistry.SchemaAwareKafka;
import com.thealtered7.schemaregistry.SchemaRegistryConfig;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableUpdatedNotificationPublisher implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(TableUpdatedNotificationPublisher.class);

    private final Producer<String, TableUpdatedNotification> producer;
    private final String topic;

    public TableUpdatedNotificationPublisher(
            String bootstrapServers, String clientId, String topic, SchemaRegistryConfig schemaRegistryConfig) {
        this(
                SchemaAwareKafka.createProducer(
                        bootstrapServers, clientId, schemaRegistryConfig, TableUpdatedNotification.class),
                topic);
    }

    TableUpdatedNotificationPublisher(Producer<String, TableUpdatedNotification> producer, String topic) {
        this.producer = producer;
        this.topic = topic;
    }

    public void publish(TableUpdatedNotification notification) {
        try {
            String key = notificationKey(notification);
            producer.send(new ProducerRecord<>(topic, key, notification));
            log.info(
                    "Published table-updated notification: topic={}, key={}, table_fqn={}, format={}",
                    topic,
                    key,
                    notification.tableFqn(),
                    notification.format());
        } catch (Exception e) {
            log.error("Failed to publish table-updated notification for {}", notification.tableFqn(), e);
        }
    }

    static String notificationKey(TableUpdatedNotification notification) {
        String bufferId = notification.extractBufferId();
        if (bufferId != null && !bufferId.isBlank()) {
            return bufferId;
        }
        return notification.tableFqn();
    }

    @Override
    public void close() {
        producer.close();
    }
}
