package com.thealtered7.datapipelines;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.thealtered7.observability.Observability;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Posts completed table writes to the datapipelines service using the JDK {@link HttpClient}.
 * All write types go to {@code POST /table-writes} with a {@code write_type} discriminator.
 *
 * <p>Each POST throws on transport failure or a non-2xx response; callers register writes on a
 * best-effort basis and log rather than fail the underlying table write.
 */
public final class DatapipelinesHttpClient implements DatapipelinesClient {

    private static final Logger log = LoggerFactory.getLogger(DatapipelinesHttpClient.class);
    private static final String TABLE_WRITES_PATH = "/table-writes";
    private static final String DEFAULT_CATALOG = "lakehouse";
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(10);
    private static final String JWT_SECRET_ENV_VAR = "DATAPIPELINES_JWT_SECRET_KEY";
    private static final Duration JWT_TOKEN_TTL = Duration.ofMinutes(5);
    private static final String JWT_SUBJECT = "open_tables_writer";

    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final Observability observability;
    private final String catalogName;
    private final URI tableWritesEndpoint;
    private final JwtTokenProvider jwtTokenProvider;

    public DatapipelinesHttpClient(
            HttpClient httpClient,
            ObjectMapper objectMapper,
            Observability observability,
            String baseUrl,
            String catalogName) {
        this(httpClient, objectMapper, observability, baseUrl, catalogName, null);
    }

    public DatapipelinesHttpClient(
            HttpClient httpClient,
            ObjectMapper objectMapper,
            Observability observability,
            String baseUrl,
            String catalogName,
            JwtTokenProvider jwtTokenProvider) {
        this.httpClient = Objects.requireNonNull(httpClient, "httpClient");
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper");
        this.observability = Objects.requireNonNull(observability, "observability");
        String base = stripTrailingSlash(Objects.requireNonNull(baseUrl, "baseUrl"));
        this.tableWritesEndpoint = URI.create(base + TABLE_WRITES_PATH);
        this.catalogName = catalogName;
        this.jwtTokenProvider = jwtTokenProvider;
    }

    /**
     * Builds a client from configuration. Returns a {@link NoopDatapipelinesClient} when
     * {@code baseUrl} is blank so registration is disabled without null checks at call sites.
     */
    public static DatapipelinesClient create(
            String baseUrl, String catalogName, boolean jwtEnabled, Observability observability) {
        if (baseUrl == null || baseUrl.isBlank()) {
            log.info("Datapipelines HTTP client disabled; no base URL configured");
            return new NoopDatapipelinesClient();
        }
        HttpClient httpClient = HttpClient.newBuilder().connectTimeout(REQUEST_TIMEOUT).build();
        JwtTokenProvider jwtTokenProvider = jwtEnabled ? buildJwtTokenProvider() : null;
        String catalog = (catalogName == null || catalogName.isBlank()) ? DEFAULT_CATALOG : catalogName.trim();
        DatapipelinesHttpClient client = new DatapipelinesHttpClient(
                httpClient, newObjectMapper(), observability, baseUrl, catalog, jwtTokenProvider);
        log.info(
                "Datapipelines HTTP client enabled; base URL {} (jwt {})",
                stripTrailingSlash(baseUrl),
                jwtEnabled ? "enabled" : "disabled");
        return client;
    }

    private static JwtTokenProvider buildJwtTokenProvider() {
        String secret = System.getenv(JWT_SECRET_ENV_VAR);
        if (secret == null || secret.isBlank()) {
            throw new IllegalStateException(
                    "datapipelines.http.jwt.enabled=true but " + JWT_SECRET_ENV_VAR + " is not set");
        }
        return new JwtTokenProvider(secret, JWT_SUBJECT, JWT_TOKEN_TTL);
    }

    private static ObjectMapper newObjectMapper() {
        return new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    @Override
    public void postTableWrite(TableWriteRegistration registration) {
        post("table_write", registration.tableName(), toRequest(registration));
    }

    private TableWriteRequest toRequest(TableWriteRegistration registration) {
        KafkaWriteContext kafka = registration.kafka();
        return new TableWriteRequest(
                registration.writeType(),
                catalogName,
                registration.namespaceName(),
                registration.databaseName(),
                registration.tableName(),
                registration.sourceInstanceName(),
                registration.sourceDatabaseName(),
                registration.sourceSchemaName(),
                registration.sourceTableName(),
                kafka == null ? null : kafka.topic(),
                kafka == null ? null : kafka.partition(),
                kafka == null ? null : kafka.offset(),
                registration.writeRowCount(),
                registration.mergeRowCount(),
                registration.rawFilePath(),
                registration.rawFileSize(),
                registration.extractJobId(),
                registration.extractBufferId(),
                registration.extractType(),
                registration.extractStartAt(),
                registration.extractEndAt(),
                registration.mergeStartAt(),
                registration.mergeEndAt(),
                registration.warehousePath());
    }

    private void post(String operation, String tableName, Object requestBody) {
        Objects.requireNonNull(requestBody, "requestBody");
        try {
            observability.observeCallableVoid(
                    Observability.DATAPIPELINES_CLIENT_PREFIX,
                    operation,
                    Map.of("table", String.valueOf(tableName)),
                    () -> {
                        send(tableWritesEndpoint, requestBody);
                        return "success";
                    });
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Failed to POST to " + tableWritesEndpoint, e);
        }
    }

    private void send(URI endpoint, Object requestBody) throws Exception {
        byte[] body = objectMapper.writeValueAsBytes(requestBody);
        HttpRequest.Builder requestBuilder = HttpRequest.newBuilder(endpoint)
                .timeout(REQUEST_TIMEOUT)
                .header("Content-Type", "application/json");
        if (jwtTokenProvider != null) {
            requestBuilder.header("Authorization", "Bearer " + jwtTokenProvider.mintToken());
        }
        HttpRequest request = requestBuilder
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .build();
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        int status = response.statusCode();
        if (status < 200 || status >= 300) {
            throw new RuntimeException(
                    "Datapipelines returned status " + status + " for " + endpoint + ": " + response.body());
        }
        log.debug("Posted table write to {} (status {})", endpoint, status);
    }

    private static String stripTrailingSlash(String url) {
        String trimmed = url.trim();
        return trimmed.endsWith("/") ? trimmed.substring(0, trimmed.length() - 1) : trimmed;
    }
}