package com.thealtered7.datapipelines;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Kafka source coordinates for a table write, forwarded from the daemon into the writer so the
 * datapipelines registration can record where the write originated. May be {@code null} for
 * one-shot or test invocations that are not driven by a Kafka record.
 */
public record KafkaWriteContext(String topic, Integer partition, Long offset) {

    public static KafkaWriteContext fromRecord(ConsumerRecord<?, ?> record) {
        return new KafkaWriteContext(record.topic(), record.partition(), record.offset());
    }
}
