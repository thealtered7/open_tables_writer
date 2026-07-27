package com.thealtered7.datapipelines;

import com.fasterxml.jackson.databind.ObjectMapper;
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
 * Bronze writes go to {@code /bronze-table-writes} and Type 2 writes to {@code /type2-table-writes}.
 *
 * <p>Each POST throws on transport failure or a non-2xx response; callers register writes on a
 * best-effort basis and log rather than fail the underlying table write.
 */
public final class DatapipelinesHttpClient implements DatapipelinesClient {

    private static final Logger log = LoggerFactory.getLogger(DatapipelinesHttpClient.class);
    private static final String BRONZE_PATH = "/bronze-table-writes";
    private static final String TYPE2_PATH = "/type2-table-writes";
    private static final String DEFAULT_CATALOG = "lakehouse";
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(10);
    private static final String JWT_SECRET_ENV_VAR = "DATAPIPELINES_JWT_SECRET_KEY";
    private static final Duration JWT_TOKEN_TTL = Duration.ofMinutes(5);
    private static final String JWT_SUBJECT = "open_tables_writer";

    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final Observability observability;
    private final String catalogName;
    private final URI bronzeEndpoint;
    private final URI type2Endpoint;
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
        this.bronzeEndpoint = URI.create(base + BRONZE_PATH);
        this.type2Endpoint = URI.create(base + TYPE2_PATH);
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
                httpClient, new ObjectMapper(), observability, baseUrl, catalog, jwtTokenProvider);
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

    @Override
    public void postBronzeTableWrite(BronzeTableWriteRegistration registration) {
        post("bronze_table_write", bronzeEndpoint, registration.tableName(), toBronzeRequest(registration));
    }

    @Override
    public void postType2TableWrite(Type2TableWriteRegistration registration) {
        post("type2_table_write", type2Endpoint, registration.tableName(), toType2Request(registration));
    }

    private BronzeTableWriteRequest toBronzeRequest(BronzeTableWriteRegistration registration) {
        KafkaWriteContext kafka = registration.kafka();
        return new BronzeTableWriteRequest(
                catalogName,
                registration.sourceInstanceName(),
                registration.sourceDatabaseName(),
                registration.sourceSchemaName(),
                registration.sourceTableName(),
                registration.databaseName(),
                registration.namespaceName(),
                registration.tableName(),
                registration.rowCount(),
                kafka == null ? null : kafka.topic(),
                kafka == null ? null : kafka.partition(),
                kafka == null ? null : kafka.offset(),
                registration.warehousePath());
    }

    private Type2TableWriteRequest toType2Request(Type2TableWriteRegistration registration) {
        KafkaWriteContext kafka = registration.kafka();
        return new Type2TableWriteRequest(
                catalogName,
                registration.sourceCatalogName(),
                registration.sourceDatabaseName(),
                registration.sourceNamespaceName(),
                registration.sourceTableName(),
                registration.databaseName(),
                registration.namespaceName(),
                registration.tableName(),
                registration.rowCount(),
                kafka == null ? null : kafka.topic(),
                kafka == null ? null : kafka.partition(),
                kafka == null ? null : kafka.offset(),
                registration.warehousePath());
    }

    private void post(String operation, URI endpoint, String tableName, Object requestBody) {
        Objects.requireNonNull(requestBody, "requestBody");
        try {
            observability.observeCallableVoid(
                    Observability.DATAPIPELINES_CLIENT_PREFIX,
                    operation,
                    Map.of("table", String.valueOf(tableName)),
                    () -> {
                        send(endpoint, requestBody);
                        return "success";
                    });
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Failed to POST to " + endpoint, e);
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
