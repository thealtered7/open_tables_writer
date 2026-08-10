package com.thealtered7;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CdcInputFileNamesTest {

    @Test
    void parsesPgoutputFlushFilename() {
        assertEquals(
                "geo.public.scalars",
                CdcInputFileNames.tableFqnFromFileName(
                        "geo.public.scalars-2026-06-07_20-07-52-983-000001.jsonl"));
    }

    @Test
    void parsesPgoutputFlushAvroFilename() {
        assertEquals(
                "geo.public.scalars",
                CdcInputFileNames.tableFqnFromFileName(
                        "geo.public.scalars-2026-06-07_20-07-52-983-000001.avro"));
    }

    @Test
    void parsesLegacyFilename() {
        assertEquals(
                "geo.public.scalars",
                CdcInputFileNames.tableFqnFromFileName("geo.public.scalars-2026-05-31_02-51-21.jsonl"));
    }

    @Test
    void stripsJsonlSuffixWhenNoTimestampPattern() {
        assertEquals("custom.table", CdcInputFileNames.tableFqnFromFileName("custom.table.jsonl"));
    }

    @Test
    void stripsAvroSuffixWhenNoTimestampPattern() {
        assertEquals("custom.table", CdcInputFileNames.tableFqnFromFileName("custom.table.avro"));
    }
}
