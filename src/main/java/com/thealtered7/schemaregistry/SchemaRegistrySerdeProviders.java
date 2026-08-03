package com.thealtered7.schemaregistry;

import java.util.Objects;

public final class SchemaRegistrySerdeProviders {

    private SchemaRegistrySerdeProviders() {}

    public static SchemaRegistrySerdeProvider create(SchemaRegistryConfig config) {
        Objects.requireNonNull(config, "config");
        config.validate();
        return switch (config.backend()) {
            case NONE -> new NoneSchemaRegistrySerdeProvider();
            case CONFLUENT -> new ConfluentSchemaRegistrySerdeProvider(config);
            case GLUE -> new GlueSchemaRegistrySerdeProvider(config);
        };
    }
}
