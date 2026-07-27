package com.thealtered7.datapipelines;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import com.thealtered7.observability.Observability;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DatapipelinesHttpClientTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

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
        server.createContext("/bronze-table-writes", this::handle);
        server.createContext("/type2-table-writes", this::handle);
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

    private static BronzeTableWriteRegistration sampleBronzeRegistration() {
        return new BronzeTableWriteRegistration(
                "test-instance",
                "geo",
                "public",
                "scalars",
                "geo",
                "public_bronze",
                "scalars",
                1000L,
                "/opt/data/icebergtable",
                new KafkaWriteContext("cdc-file-write", 2, 42L));
    }

    private static Type2TableWriteRegistration sampleType2Registration() {
        return new Type2TableWriteRegistration(
                "lakehouse",
                "geo",
                "public_bronze",
                "scalars",
                "geo",
                "public_silver",
                "scalars_type2",
                1000L,
                "/opt/data/silver",
                new KafkaWriteContext("open-table-write-notifications", 1, 7L));
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

        client.postBronzeTableWrite(sampleBronzeRegistration());

        assertEquals("/bronze-table-writes", capturedPath.get());
    }

    @Test
    void postBronzeTableWrite_sendsSnakeCaseJson() throws Exception {
        newClient().postBronzeTableWrite(sampleBronzeRegistration());

        assertEquals("POST", capturedMethod.get());
        assertEquals("/bronze-table-writes", capturedPath.get());
        assertEquals("application/json", capturedContentType.get());
        assertNull(capturedAuthorization.get());

        JsonNode body = MAPPER.readTree(capturedBody.get());
        assertEquals("lakehouse", body.get("catalog_name").asText());
        assertEquals("test-instance", body.get("source_instance_name").asText());
        assertEquals("geo", body.get("source_database_name").asText());
        assertEquals("public", body.get("source_schema_name").asText());
        assertEquals("scalars", body.get("source_table_name").asText());
        assertEquals("geo", body.get("database_name").asText());
        assertEquals("public_bronze", body.get("namespace_name").asText());
        assertEquals("scalars", body.get("table_name").asText());
        assertEquals(1000L, body.get("row_count").asLong());
        assertEquals("cdc-file-write", body.get("kafka_topic").asText());
        assertEquals(2, body.get("kafka_partition").asInt());
        assertEquals(42L, body.get("kafka_offset").asLong());
        assertEquals("/opt/data/icebergtable", body.get("warehouse_path").asText());
    }

    @Test
    void postType2TableWrite_sendsSnakeCaseJson() throws Exception {
        newClient().postType2TableWrite(sampleType2Registration());

        assertEquals("POST", capturedMethod.get());
        assertEquals("/type2-table-writes", capturedPath.get());

        JsonNode body = MAPPER.readTree(capturedBody.get());
        assertEquals("lakehouse", body.get("catalog_name").asText());
        assertEquals("lakehouse", body.get("source_catalog_name").asText());
        assertEquals("geo", body.get("source_database_name").asText());
        assertEquals("public_bronze", body.get("source_namespace_name").asText());
        assertEquals("scalars", body.get("source_table_name").asText());
        assertEquals("geo", body.get("database_name").asText());
        assertEquals("public_silver", body.get("namespace_name").asText());
        assertEquals("scalars_type2", body.get("table_name").asText());
        assertEquals("/opt/data/silver", body.get("warehouse_path").asText());
    }

    @Test
    void post_omitsNullKafkaFields() throws Exception {
        newClient().postBronzeTableWrite(new BronzeTableWriteRegistration(
                "test-instance", "geo", "public", "scalars", "geo", "public_bronze", "scalars", 5L, null, null));

        JsonNode body = MAPPER.readTree(capturedBody.get());
        assertNull(body.get("kafka_topic"));
        assertNull(body.get("kafka_partition"));
        assertNull(body.get("kafka_offset"));
        assertNull(body.get("warehouse_path"));
    }

    @Test
    void post_throwsOnNonSuccessStatus() {
        responseStatus.set(500);
        DatapipelinesHttpClient client = newClient();
        assertThrows(RuntimeException.class, () -> client.postBronzeTableWrite(sampleBronzeRegistration()));
    }

    @Test
    void post_addsBearerTokenWhenJwtProviderPresent() {
        JwtTokenProvider provider = new JwtTokenProvider(
                "test-secret-key-that-is-at-least-32-bytes-long!!",
                "open_tables_writer",
                Duration.ofMinutes(5));
        DatapipelinesHttpClient client = new DatapipelinesHttpClient(
                HttpClient.newHttpClient(), MAPPER, Observability.noop(), baseUrl, "lakehouse", provider);

        client.postType2TableWrite(sampleType2Registration());

        String authorization = capturedAuthorization.get();
        assertTrue(authorization != null && authorization.startsWith("Bearer "));
        assertEquals(3, authorization.substring("Bearer ".length()).split("\\.").length);
    }
}
