package com.thealtered7;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.thealtered7.models.DeadLetterMessage;
import com.thealtered7.models.FileFlushNotification;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

class DeadLetterPublisherTest {

    @Test
    void publish_sendsWrappedMessage() {
        MockProducer<String, DeadLetterMessage> producer =
                new MockProducer<>(true, new StringSerializer(), identitySerializer());
        DeadLetterPublisher publisher = new DeadLetterPublisher(producer, "cdc-file-write.dlq");

        FileFlushNotification payload = new FileFlushNotification(
                "raw",
                "/tmp/a.jsonl",
                "geo.public.t",
                "job-1",
                "buf-1",
                "cdc",
                null,
                null,
                1L,
                10L,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null);
        ConsumerRecord<String, FileFlushNotification> record =
                new ConsumerRecord<>("cdc-file-write", 2, 9L, "key", payload);

        publisher.publish(record, new IllegalStateException("boom"), "job-1", "buf-1", "geo.public.t");

        assertEquals(1, producer.history().size());
        ProducerRecord<String, DeadLetterMessage> sent = producer.history().get(0);
        assertEquals("cdc-file-write.dlq", sent.topic());
        DeadLetterMessage message = sent.value();
        assertEquals("cdc-file-write", message.originalTopic());
        assertEquals(2, message.originalPartition());
        assertEquals(9L, message.originalOffset());
        assertEquals("job-1", message.extractJobId());
        assertEquals("buf-1", message.extractBufferId());
        assertEquals("geo.public.t", message.tableIdentity());
        assertEquals(IllegalStateException.class.getName(), message.exceptionClass());
        assertEquals("boom", message.exceptionMessage());
        assertTrue(message.exceptionStackTrace().contains("IllegalStateException"));
    }

    @Test
    void publish_failurePropagatesWithoutSwallowing() {
        MockProducer<String, DeadLetterMessage> producer =
                new MockProducer<>(false, new StringSerializer(), identitySerializer());
        producer.sendException = new RuntimeException("broker down");
        DeadLetterPublisher publisher = new DeadLetterPublisher(producer, "t.dlq");
        ConsumerRecord<String, String> record = new ConsumerRecord<>("t", 0, 1L, "k", "v");

        assertThrows(
                IllegalStateException.class,
                () -> publisher.publish(record, new RuntimeException("orig"), null, null, null));
    }

    @Test
    void extractMdc_putsAndClears() {
        ExtractMdc.put("job-a", "buf-b");
        assertEquals("job-a", org.slf4j.MDC.get(ExtractMdc.EXTRACT_JOB_ID));
        assertEquals("buf-b", org.slf4j.MDC.get(ExtractMdc.EXTRACT_BUFFER_ID));
        ExtractMdc.clear();
        assertNull(org.slf4j.MDC.get(ExtractMdc.EXTRACT_JOB_ID));
        assertNull(org.slf4j.MDC.get(ExtractMdc.EXTRACT_BUFFER_ID));
        assertEquals("unknown", ExtractMdc.normalize(null));
    }

    private static Serializer<DeadLetterMessage> identitySerializer() {
        return new Serializer<>() {
            @Override
            public byte[] serialize(String topic, DeadLetterMessage data) {
                return new byte[0];
            }
        };
    }
}
