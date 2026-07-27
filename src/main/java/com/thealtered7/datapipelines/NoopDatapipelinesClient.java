package com.thealtered7.datapipelines;

/** Disabled client used when no datapipelines base URL is configured. */
public final class NoopDatapipelinesClient implements DatapipelinesClient {

    @Override
    public void postBronzeTableWrite(BronzeTableWriteRegistration registration) {
        // intentionally disabled
    }

    @Override
    public void postType2TableWrite(Type2TableWriteRegistration registration) {
        // intentionally disabled
    }
}
