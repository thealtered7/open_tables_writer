package com.thealtered7;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertTrue;

class GradleBuildTest {

    private static final Logger log = LoggerFactory.getLogger(GradleBuildTest.class);

    @Test
    void projectBuilds() {
        log.info("project builds");
        assertTrue(true);
    }
}
