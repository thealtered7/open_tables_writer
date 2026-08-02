package com.thealtered7.datapipelines;

/**
 * Registers completed open-table writes with the datapipelines service. Implementations post to
 * {@code POST /table-writes}; a no-op implementation is used when registration is disabled so
 * callers never need null checks.
 */
public interface DatapipelinesClient {

    void postTableWrite(TableWriteRegistration registration);
}
