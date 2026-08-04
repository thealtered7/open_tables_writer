package com.thealtered7;

import org.slf4j.MDC;

/** MDC keys for Kafka-driven extract processing. */
public final class ExtractMdc {

    public static final String EXTRACT_JOB_ID = "extract_job_id";
    public static final String EXTRACT_BUFFER_ID = "extract_buffer_id";
    public static final String UNKNOWN = "unknown";

    private ExtractMdc() {}

    public static void put(String extractJobId, String extractBufferId) {
        MDC.put(EXTRACT_JOB_ID, normalize(extractJobId));
        MDC.put(EXTRACT_BUFFER_ID, normalize(extractBufferId));
    }

    public static void clear() {
        MDC.remove(EXTRACT_JOB_ID);
        MDC.remove(EXTRACT_BUFFER_ID);
    }

    public static String normalize(String value) {
        return value == null || value.isBlank() ? UNKNOWN : value;
    }
}
