package com.thealtered7;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class InputFileWaiter {

    private static final Logger log = LoggerFactory.getLogger(InputFileWaiter.class);
    public static final Duration DEFAULT_MAX_WAIT = Duration.ofSeconds(10);
    public static final Duration DEFAULT_POLL_INTERVAL = Duration.ofMillis(500);

    private InputFileWaiter() {}

    public static boolean waitForFile(Path path, Duration maxWait, Duration pollInterval) {
        long deadlineNanos = System.nanoTime() + maxWait.toNanos();
        while (System.nanoTime() < deadlineNanos) {
            if (Files.isRegularFile(path)) {
                return true;
            }
            try {
                Thread.sleep(pollInterval.toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Interrupted while waiting for input file: {}", path);
                return Files.isRegularFile(path);
            }
        }
        return Files.isRegularFile(path);
    }

    public static void requireFile(Path path) throws IOException {
        if (!Files.isRegularFile(path)) {
            throw new IOException("Input file does not exist: " + path);
        }
    }
}
