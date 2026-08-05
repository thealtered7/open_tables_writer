package com.thealtered7.schemaregistry;

/** No-op registrar used when {@code schema.registry.type=none}. */
final class NoneTableSchemaRegistrar implements TableSchemaRegistrar {

    @Override
    public String register(String subject, String sparkSchemaJson) {
        return null;
    }
}
