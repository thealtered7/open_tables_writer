package com.thealtered7.datapipelines;

/**
 * Registers completed open-table writes with the datapipelines service. Implementations post to
 * the bronze and Type 2 endpoints; a no-op implementation is used when registration is disabled so
 * callers never need null checks.
 */
public interface DatapipelinesClient {

    void postBronzeTableWrite(BronzeTableWriteRegistration registration);

    void postType2TableWrite(Type2TableWriteRegistration registration);
}
