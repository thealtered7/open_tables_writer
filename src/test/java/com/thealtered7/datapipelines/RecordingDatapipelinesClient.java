package com.thealtered7.datapipelines;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/** Test double that records registrations and can optionally throw from POST methods. */
public final class RecordingDatapipelinesClient implements DatapipelinesClient {

    private final List<BronzeTableWriteRegistration> bronzeWrites = new ArrayList<>();
    private final List<Type2TableWriteRegistration> type2Writes = new ArrayList<>();
    private final AtomicBoolean failPosts = new AtomicBoolean(false);

    public void failPosts() {
        failPosts.set(true);
    }

    public List<BronzeTableWriteRegistration> bronzeWrites() {
        return List.copyOf(bronzeWrites);
    }

    public List<Type2TableWriteRegistration> type2Writes() {
        return List.copyOf(type2Writes);
    }

    @Override
    public void postBronzeTableWrite(BronzeTableWriteRegistration registration) {
        if (failPosts.get()) {
            throw new RuntimeException("forced bronze post failure");
        }
        bronzeWrites.add(registration);
    }

    @Override
    public void postType2TableWrite(Type2TableWriteRegistration registration) {
        if (failPosts.get()) {
            throw new RuntimeException("forced type2 post failure");
        }
        type2Writes.add(registration);
    }
}
