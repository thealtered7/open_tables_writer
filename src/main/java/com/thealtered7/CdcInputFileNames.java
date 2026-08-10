package com.thealtered7;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class CdcInputFileNames {

    // pgoutput current: {table}-{yyyy-MM-dd_HH-mm-ss-SSS}-{seq}.jsonl|.avro
    private static final Pattern PGOUTPUT_FLUSH = Pattern.compile(
            "^(.+)-(\\d{4}-\\d{2}-\\d{2}_\\d{2}-\\d{2}-\\d{2}-\\d{3})-(\\d{6})\\.(jsonl|avro)$");

    // legacy test/sample files
    private static final Pattern LEGACY =
            Pattern.compile("^(.+)-(\\d{4}-\\d{2}-\\d{2}_\\d{2}-\\d{2}-\\d{2})\\.(jsonl|avro)$");

    private CdcInputFileNames() {}

    public static String tableFqnFromFileName(String fileName) {
        Matcher matcher = PGOUTPUT_FLUSH.matcher(fileName);
        if (matcher.matches()) {
            return matcher.group(1);
        }
        matcher = LEGACY.matcher(fileName);
        if (matcher.matches()) {
            return matcher.group(1);
        }
        return fileName.replaceFirst("\\.(jsonl|avro)$", "");
    }
}
