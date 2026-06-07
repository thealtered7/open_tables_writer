package com.thealtered7;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class InputFileWaiterTest {

    @Test
    void returnsTrueWhenFileExistsImmediately(@TempDir Path tempDir) throws IOException {
        Path file = tempDir.resolve("data.jsonl");
        Files.writeString(file, "{}");

        assertTrue(InputFileWaiter.waitForFile(file, Duration.ofSeconds(1), Duration.ofMillis(50)));
    }

    @Test
    void returnsTrueWhenFileAppearsWithinWaitWindow(@TempDir Path tempDir) {
        Path file = tempDir.resolve("delayed.jsonl");

        Thread writer = new Thread(() -> {
            try {
                Thread.sleep(200);
                Files.writeString(file, "{}");
            } catch (IOException | InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        writer.start();

        assertTrue(InputFileWaiter.waitForFile(file, Duration.ofSeconds(2), Duration.ofMillis(50)));
    }

    @Test
    void returnsFalseWhenFileNeverAppears(@TempDir Path tempDir) {
        Path file = tempDir.resolve("missing.jsonl");

        assertFalse(InputFileWaiter.waitForFile(file, Duration.ofMillis(200), Duration.ofMillis(50)));
    }

    @Test
    void requireFileThrowsWhenMissing(@TempDir Path tempDir) {
        Path file = tempDir.resolve("missing.jsonl");

        assertThrows(IOException.class, () -> InputFileWaiter.requireFile(file));
    }
}
