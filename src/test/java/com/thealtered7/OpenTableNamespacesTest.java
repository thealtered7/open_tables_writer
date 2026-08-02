package com.thealtered7;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class OpenTableNamespacesTest {

    @Test
    void bronzeAppendsSuffix() {
        assertEquals("public_bronze", OpenTableNamespaces.bronze("public"));
    }

    @Test
    void silverAppendsSuffix() {
        assertEquals("public_silver", OpenTableNamespaces.silver("public"));
    }

    @Test
    void silverFromBronzeRewritesSuffix() {
        assertEquals("public_silver", OpenTableNamespaces.silverFromBronze("public_bronze"));
    }

    @Test
    void type2TableAppendsSuffix() {
        assertEquals("scalars_type2", OpenTableNamespaces.type2Table("scalars"));
    }

    @Test
    void type1TableAppendsSuffix() {
        assertEquals("scalars_type1", OpenTableNamespaces.type1Table("scalars"));
    }

    @Test
    void toBronzeTableFqnRewritesSchemaSegment() {
        assertEquals("geo.public_bronze.scalars", OpenTableNamespaces.toBronzeTableFqn("geo.public.scalars"));
    }

    @Test
    void toBronzeTableFqnRejectsMalformedFqn() {
        assertThrows(IllegalArgumentException.class, () -> OpenTableNamespaces.toBronzeTableFqn("geo.public"));
    }
}
