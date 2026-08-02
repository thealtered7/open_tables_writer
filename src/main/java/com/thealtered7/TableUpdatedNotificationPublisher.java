package com.thealtered7;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.thealtered7.models.TableUpdatedNotification;
import com.thealtered7.models.TableUpdatedNotificationJson;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableUpdatedNotificationPublisher implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(TableUpdatedNotificationPublisher.class);
    private static final ObjectMapper OBJECT_MAPPER = TableUpdatedNotificationJson.MAPPER;

    private final Producer<String, String> producer;
    private final String topic;

    public TableUpdatedNotificationPublisher(String bootstrapServers, String clientId, String topic) {
        this(createProducer(bootstrapServers, clientId), topic);
    }

    TableUpdatedNotificationPublisher(Producer<String, String> producer, String topic) {
        this.producer = producer;
        this.topic = topic;
    }

    private static Producer<String, String> createProducer(String bootstrapServers, String clientId) {
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(producerProps);
    }

    public void publish(TableUpdatedNotification notification) {
        try {
            String value = OBJECT_MAPPER.writeValueAsString(notification);
            String key = notificationKey(notification);
            producer.send(new ProducerRecord<>(topic, key, value));
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
