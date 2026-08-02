package com.thealtered7.datapipelines;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/** Test double that records registrations and can optionally throw from POST methods. */
public final class RecordingDatapipelinesClient implements DatapipelinesClient {

    private final List<TableWriteRegistration> writes = new ArrayList<>();
    private final AtomicBoolean failPosts = new AtomicBoolean(false);

    public void failPosts() {
        failPosts.set(true);
    }

    public List<TableWriteRegistration> writes() {
        return List.copyOf(writes);
    }

    public List<TableWriteRegistration> bronzeWrites() {
        return writes.stream()
                .filter(w -> TableWriteRegistration.WRITE_TYPE_BRONZE.equals(w.writeType()))
                .toList();
    }

    public List<TableWriteRegistration> type1Writes() {
        return writes.stream()
                .filter(w -> TableWriteRegistration.WRITE_TYPE_SILVER_TYPE_1.equals(w.writeType()))
                .toList();
    }

    public List<TableWriteRegistration> type2Writes() {
        return writes.stream()
                .filter(w -> TableWriteRegistration.WRITE_TYPE_SILVER_TYPE_2.equals(w.writeType()))
                .toList();
    }

    @Override
    public void postTableWrite(TableWriteRegistration registration) {
        if (failPosts.get()) {
            throw new RuntimeException("forced table write post failure");
        }
        writes.add(registration);
    }
}
