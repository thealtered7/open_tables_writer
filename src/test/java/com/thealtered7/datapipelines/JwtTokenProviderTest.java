package com.thealtered7.datapipelines;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class JwtTokenProviderTest {

    @Test
    void rejectsSecretShorterThan32Bytes() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new JwtTokenProvider("too-short", "open_tables_writer", Duration.ofMinutes(5)));
    }

    @Test
    void mintsCompactJwtWithThreeSegments() {
        JwtTokenProvider provider = new JwtTokenProvider(
                "test-secret-key-that-is-at-least-32-bytes-long!!",
                "open_tables_writer",
                Duration.ofMinutes(5));

        String token = provider.mintToken();
        assertTrue(token.split("\\.").length == 3);
    }
}
