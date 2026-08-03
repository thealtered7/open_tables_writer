package com.thealtered7.datapipelines;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import com.thealtered7.observability.Observability;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DatapipelinesHttpClientTest {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    private HttpServer server;
    private String baseUrl;
    private final AtomicReference<String> capturedMethod = new AtomicReference<>();
    private final AtomicReference<String> capturedPath = new AtomicReference<>();
    private final AtomicReference<String> capturedContentType = new AtomicReference<>();
    private final AtomicReference<String> capturedAuthorization = new AtomicReference<>();
    private final AtomicReference<String> capturedBody = new AtomicReference<>();
    private final AtomicInteger responseStatus = new AtomicInteger(201);

    @BeforeEach
    void setUp() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/table-writes", this::handle);
        server.start();
        baseUrl = "http://127.0.0.1:" + server.getAddress().getPort();
    }

    @AfterEach
    void tearDown() {
        server.stop(0);
    }

    private void handle(HttpExchange exchange) throws IOException {
        capturedMethod.set(exchange.getRequestMethod());
        capturedPath.set(exchange.getRequestURI().getPath());
        capturedContentType.set(exchange.getRequestHeaders().getFirst("Content-Type"));
        capturedAuthorization.set(exchange.getRequestHeaders().getFirst("Authorization"));
        try (InputStream in = exchange.getRequestBody()) {
            capturedBody.set(new String(in.readAllBytes(), StandardCharsets.UTF_8));
        }
        int status = responseStatus.get();
        exchange.sendResponseHeaders(status, -1);
        exchange.close();
    }

    private DatapipelinesHttpClient newClient() {
        return new DatapipelinesHttpClient(
                HttpClient.newHttpClient(), MAPPER, Observability.noop(), baseUrl, "lakehouse");
    }

    private static TableWriteRegistration sampleBronzeRegistration() {
        return new TableWriteRegistration(
                TableWriteRegistration.WRITE_TYPE_BRONZE,
                "geo",
                "public_bronze",
                "scalars",
                "test-instance",
                "geo",
                "public",
                "scalars",
                1000L,
                null,
                "/opt/data/raw/geo/public/scalars/file.jsonl",
                2048L,
                "job-bronze",
                "buf-bronze",
                "cdc",
                Instant.parse("2026-05-31T02:50:21Z"),
                Instant.parse("2026-05-31T02:51:21Z"),
                null,
                null,
                "/opt/data/icebergtable",
                new KafkaWriteContext("cdc-file-write", 2, 42L),
                "{\"type\":\"struct\",\"fields\":[]}",
                "{\"type\":\"struct\",\"name\":\"envelope\"}",
                "1",
                "2");
    }

    private static TableWriteRegistration sampleType1Registration() {
        return new TableWriteRegistration(
                TableWriteRegistration.WRITE_TYPE_SILVER_TYPE_1,
                "geo",
                "public_silver",
                "scalars_type1",
                "test-instance",
                "geo",
                "public",
                "scalars",
                1000L,
                1000L,
                "/opt/data/raw/geo/public/scalars/file.jsonl",
                2048L,
                "job-1",
                "buf-1",
                "cdc",
                Instant.parse("2026-05-31T02:51:21.544690220Z"),
                Instant.parse("2026-05-31T02:52:21.544690220Z"),
                Instant.parse("2026-05-31T02:53:00Z"),
                Instant.parse("2026-05-31T02:53:05Z"),
                "/opt/data/silver",
                new KafkaWriteContext("open-table-write-notifications", 1, 7L),
                null,
                null,
                null,
                null);
    }

    private static TableWriteRegistration sampleType2Registration() {
        return new TableWriteRegistration(
                TableWriteRegistration.WRITE_TYPE_SILVER_TYPE_2,
                "geo",
                "public_silver",
                "scalars_type2",
                "test-instance",
                "geo",
                "public",
                "scalars",
                1000L,
                1000L,
                "/opt/data/raw/geo/public/scalars/file.jsonl",
                2048L,
                "job-1",
                "buf-1",
                "cdc",
                Instant.parse("2026-05-31T02:51:21.544690220Z"),
                Instant.parse("2026-05-31T02:52:21.544690220Z"),
                Instant.parse("2026-05-31T02:53:00Z"),
                Instant.parse("2026-05-31T02:53:05Z"),
                "/opt/data/silver",
                new KafkaWriteContext("open-table-write-notifications", 1, 7L),
                null,
                null,
                null,
                null);
    }

    @Test
    void create_returnsNoopWhenBaseUrlBlank() {
        DatapipelinesClient client =
                DatapipelinesHttpClient.create("  ", "lakehouse", false, Observability.noop());
        assertInstanceOf(NoopDatapipelinesClient.class, client);
    }

    @Test
    void create_acceptsTrailingSlashBaseUrl() throws Exception {
        DatapipelinesHttpClient client = new DatapipelinesHttpClient(
                HttpClient.newHttpClient(), MAPPER, Observability.noop(), baseUrl + "/", "lakehouse");

        client.postTableWrite(sampleBronzeRegistration());

        assertEquals("/table-writes", capturedPath.get());
    }

    @Test
    void postTableWrite_bronze_sendsSnakeCaseJson() throws Exception {
        newClient().postTableWrite(sampleBronzeRegistration());

        assertEquals("POST", capturedMethod.get());
        assertEquals("/table-writes", capturedPath.get());
        assertEquals("application/json", capturedContentType.get());
        assertNull(capturedAuthorization.get());

        JsonNode body = MAPPER.readTree(capturedBody.get());
        assertEquals("bronze", body.get("write_type").asText());
        assertEquals("lakehouse", body.get("catalog_name").asText());
        assertEquals("test-instance", body.get("source_instance_name").asText());
        assertEquals("geo", body.get("source_database_name").asText());
        assertEquals("public", body.get("source_schema_name").asText());
        assertEquals("scalars", body.get("source_table_name").asText());
        assertEquals("geo", body.get("database_name").asText());
        assertEquals("public_bronze", body.get("namespace_name").asText());
        assertEquals("scalars", body.get("table_name").asText());
        assertEquals(1000L, body.get("write_row_count").asLong());
        assertNull(body.get("merge_row_count"));
        assertEquals("/opt/data/raw/geo/public/scalars/file.jsonl", body.get("raw_file_path").asText());
        assertEquals(2048L, body.get("raw_file_size").asLong());
        assertNull(body.get("merge_start_at"));
        assertNull(body.get("merge_end_at"));
        assertEquals("cdc-file-write", body.get("kafka_topic").asText());
        assertEquals(2, body.get("kafka_partition").asInt());
        assertEquals(42L, body.get("kafka_offset").asLong());
        assertEquals("/opt/data/icebergtable", body.get("warehouse_path").asText());
        assertEquals("{\"type\":\"struct\",\"fields\":[]}", body.get("key_schema").asText());
        assertEquals("{\"type\":\"struct\",\"name\":\"envelope\"}", body.get("value_schema").asText());
        assertEquals("1", body.get("key_schema_id").asText());
        assertEquals("2", body.get("value_schema_id").asText());
    }

    @Test
    void postTableWrite_type1_sendsSnakeCaseJson() throws Exception {
        newClient().postTableWrite(sampleType1Registration());

        assertEquals("POST", capturedMethod.get());
        assertEquals("/table-writes", capturedPath.get());

        JsonNode body = MAPPER.readTree(capturedBody.get());
        assertEquals("silver_type_1", body.get("write_type").asText());
        assertEquals("lakehouse", body.get("catalog_name").asText());
        assertEquals("test-instance", body.get("source_instance_name").asText());
        assertEquals("geo", body.get("source_database_name").asText());
        assertEquals("public", body.get("source_schema_name").asText());
        assertEquals("scalars", body.get("source_table_name").asText());
        assertEquals("geo", body.get("database_name").asText());
        assertEquals("public_silver", body.get("namespace_name").asText());
        assertEquals("scalars_type1", body.get("table_name").asText());
        assertEquals(1000L, body.get("write_row_count").asLong());
        assertEquals(1000L, body.get("merge_row_count").asLong());
        assertEquals("/opt/data/raw/geo/public/scalars/file.jsonl", body.get("raw_file_path").asText());
        assertEquals(2048L, body.get("raw_file_size").asLong());
        assertEquals("2026-05-31T02:53:00Z", body.get("merge_start_at").asText());
        assertEquals("2026-05-31T02:53:05Z", body.get("merge_end_at").asText());
        assertEquals("/opt/data/silver", body.get("warehouse_path").asText());
        assertEquals("job-1", body.get("extract_job_id").asText());
    }

    @Test
    void postTableWrite_type2_sendsSnakeCaseJson() throws Exception {
        newClient().postTableWrite(sampleType2Registration());

        assertEquals("POST", capturedMethod.get());
        assertEquals("/table-writes", capturedPath.get());

        JsonNode body = MAPPER.readTree(capturedBody.get());
        assertEquals("silver_type_2", body.get("write_type").asText());
        assertEquals("lakehouse", body.get("catalog_name").asText());
        assertEquals("test-instance", body.get("source_instance_name").asText());
        assertEquals("geo", body.get("source_database_name").asText());
        assertEquals("public", body.get("source_schema_name").asText());
        assertEquals("scalars", body.get("source_table_name").asText());
        assertEquals("geo", body.get("database_name").asText());
        assertEquals("public_silver", body.get("namespace_name").asText());
        assertEquals("scalars_type2", body.get("table_name").asText());
        assertEquals("/opt/data/raw/geo/public/scalars/file.jsonl", body.get("raw_file_path").asText());
        assertEquals(2048L, body.get("raw_file_size").asLong());
        assertEquals("2026-05-31T02:53:00Z", body.get("merge_start_at").asText());
        assertEquals("2026-05-31T02:53:05Z", body.get("merge_end_at").asText());
        assertEquals("/opt/data/silver", body.get("warehouse_path").asText());
    }

    @Test
    void post_omitsNullKafkaFields() throws Exception {
        newClient().postTableWrite(new TableWriteRegistration(
                TableWriteRegistration.WRITE_TYPE_BRONZE,
                "geo",
                "public_bronze",
                "scalars",
                "test-instance",
                "geo",
                "public",
                "scalars",
                5L,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null));

        JsonNode body = MAPPER.readTree(capturedBody.get());
        assertNull(body.get("kafka_topic"));
        assertNull(body.get("kafka_partition"));
        assertNull(body.get("kafka_offset"));
        assertNull(body.get("warehouse_path"));
        assertNull(body.get("merge_row_count"));
        assertNull(body.get("key_schema"));
        assertNull(body.get("value_schema"));
        assertNull(body.get("key_schema_id"));
        assertNull(body.get("value_schema_id"));
    }

    @Test
    void post_throwsOnNonSuccessStatus() {
        responseStatus.set(500);
        DatapipelinesHttpClient client = newClient();
        assertThrows(RuntimeException.class, () -> client.postTableWrite(sampleBronzeRegistration()));
    }

    @Test
    void post_addsBearerTokenWhenJwtProviderPresent() {
        JwtTokenProvider provider = new JwtTokenProvider(
                "test-secret-key-that-is-at-least-32-bytes-long!!",
                "open_tables_writer",
                Duration.ofMinutes(5));
        DatapipelinesHttpClient client = new DatapipelinesHttpClient(
                HttpClient.newHttpClient(), MAPPER, Observability.noop(), baseUrl, "lakehouse", provider);

        client.postTableWrite(sampleType2Registration());

        String authorization = capturedAuthorization.get();
        assertTrue(authorization != null && authorization.startsWith("Bearer "));
        assertEquals(3, authorization.substring("Bearer ".length()).split("\\.").length);
    }
}
