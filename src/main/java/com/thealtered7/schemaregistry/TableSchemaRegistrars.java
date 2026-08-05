package com.thealtered7.schemaregistry;

import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Factory for {@link TableSchemaRegistrar} implementations. */
public final class TableSchemaRegistrars {

    private static final Logger log = LoggerFactory.getLogger(TableSchemaRegistrars.class);

    private TableSchemaRegistrars() {}

    public static TableSchemaRegistrar create(SchemaRegistryConfig config) {
        Objects.requireNonNull(config, "config");
        return switch (config.backend()) {
            case NONE -> new NoneTableSchemaRegistrar();
            case CONFLUENT -> new ConfluentTableSchemaRegistrar(config);
            case GLUE -> {
                log.warn("Table schema registration with Glue is not implemented; schema ids will be null");
                yield new GlueTableSchemaRegistrar();
            }
        };
    }
}
