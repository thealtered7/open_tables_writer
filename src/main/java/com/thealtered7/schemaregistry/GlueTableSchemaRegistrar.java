package com.thealtered7.schemaregistry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Stub Glue registrar; returns null until implemented. */
final class GlueTableSchemaRegistrar implements TableSchemaRegistrar {

    private static final Logger log = LoggerFactory.getLogger(GlueTableSchemaRegistrar.class);
    private boolean warned;

    @Override
    public String register(String subject, String sparkSchemaJson) {
        if (!warned) {
            log.warn("Glue table schema registration is unsupported; leaving schema ids null");
            warned = true;
        }
        return null;
    }
}
