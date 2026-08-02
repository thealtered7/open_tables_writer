package com.thealtered7.datapipelines;

/** Disabled client used when no datapipelines base URL is configured. */
public final class NoopDatapipelinesClient implements DatapipelinesClient {

    @Override
    public void postTableWrite(TableWriteRegistration registration) {
        // intentionally disabled
    }
}
