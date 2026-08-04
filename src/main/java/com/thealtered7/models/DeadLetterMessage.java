package com.thealtered7.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;

/** Wrapper written to a pipeline dead-letter topic when main-topic processing fails. */
public record DeadLetterMessage(
        @JsonProperty("original_topic") String originalTopic,
        @JsonProperty("original_partition") int originalPartition,
        @JsonProperty("original_offset") long originalOffset,
        @JsonProperty("failed_at") Instant failedAt,
        @JsonProperty("exception_class") String exceptionClass,
        @JsonProperty("exception_message") String exceptionMessage,
        @JsonProperty("exception_stack_trace") String exceptionStackTrace,
        @JsonProperty("extract_job_id") String extractJobId,
        @JsonProperty("extract_buffer_id") String extractBufferId,
        @JsonProperty("table_identity") String tableIdentity,
        @JsonProperty("original_payload") JsonNode originalPayload) {}
