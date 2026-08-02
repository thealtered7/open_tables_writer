package com.thealtered7;

import com.thealtered7.datapipelines.TableWriteRegistration;
import com.thealtered7.models.TableUpdatedNotification;
import com.thealtered7.models.TableUpdatedNotification.OpenTableFormat;
import java.time.Instant;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TableUpdatedNotificationPublisherTest {

    @Test
    void notificationKeyPrefersExtractBufferId() {
        TableUpdatedNotification withBuffer = notification("buf-42");
        assertEquals("buf-42", TableUpdatedNotificationPublisher.notificationKey(withBuffer));
    }

    @Test
    void notificationKeyFallsBackToTableFqnWhenBufferBlank() {
        TableUpdatedNotification withoutBuffer = notification("  ");
        assertEquals("geo.public_bronze.scalars", TableUpdatedNotificationPublisher.notificationKey(withoutBuffer));

        TableUpdatedNotification nullBuffer = notification(null);
        assertEquals("geo.public_bronze.scalars", TableUpdatedNotificationPublisher.notificationKey(nullBuffer));
    }

    private static TableUpdatedNotification notification(String extractBufferId) {
        return new TableUpdatedNotification(
                TableWriteRegistration.WRITE_TYPE_BRONZE,
                "geo.public_bronze.scalars",
                "/opt/data/iceberg/geo/public_bronze/scalars",
                OpenTableFormat.ICEBERG,
                "lakehouse",
                "geo",
                "public_bronze",
                "scalars",
                "/opt/data/iceberg",
                "test-instance",
                "geo",
                "public",
                "scalars",
                "/opt/data/raw/file.jsonl",
                100L,
                "job-1",
                extractBufferId,
                "cdc",
                Instant.parse("2026-05-31T02:50:21Z"),
                Instant.parse("2026-05-31T02:51:21Z"));
    }
}
