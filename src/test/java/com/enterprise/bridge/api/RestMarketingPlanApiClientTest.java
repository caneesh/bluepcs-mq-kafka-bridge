package com.enterprise.bridge.api;

import com.enterprise.bridge.model.ParsedPayload;
import com.enterprise.bridge.security.JwtTokenProvider;
import okhttp3.OkHttpClient;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RestMarketingPlanApiClientTest {

    private MockWebServer mockServer;
    private RestMarketingPlanApiClient client;
    private OkHttpClient httpClient;

    @Mock
    private JwtTokenProvider jwtTokenProvider;

    @BeforeEach
    void setUp() throws IOException {
        mockServer = new MockWebServer();
        mockServer.start();
        httpClient = new OkHttpClient.Builder()
                .connectTimeout(5, TimeUnit.SECONDS)
                .readTimeout(5, TimeUnit.SECONDS)
                .build();
        when(jwtTokenProvider.getToken()).thenReturn("test-jwt-token");
    }

    @AfterEach
    void tearDown() throws IOException {
        mockServer.shutdown();
    }

    @Nested
    @DisplayName("Successful Enrichment")
    class SuccessfulEnrichment {

        @Test
        @DisplayName("should enrich payload successfully")
        void shouldEnrichPayload() throws InterruptedException {
            mockServer.enqueue(new MockResponse()
                    .setBody("{\"marketingPlanId\":\"MP-123\",\"campaignId\":\"CAMP-456\",\"extra\":\"data\"}")
                    .setHeader("Content-Type", "application/json"));

            client = createClient();
            ParsedPayload payload = createPayload("ENT-001");

            MarketingPlanApiClient.EnrichmentResult result = client.enrich(payload);

            assertThat(result.getMarketingPlanId()).isEqualTo("MP-123");
            assertThat(result.getCampaignId()).isEqualTo("CAMP-456");
            assertThat(result.getAdditionalData()).containsEntry("extra", "data");

            RecordedRequest request = mockServer.takeRequest();
            assertThat(request.getMethod()).isEqualTo("GET");
            assertThat(request.getPath()).isEqualTo("/api/v1/marketing-plans/ENT-001");
            assertThat(request.getHeader("Authorization")).isEqualTo("Bearer test-jwt-token");
        }

        @Test
        @DisplayName("should handle numeric fields in response")
        void shouldHandleNumericFields() {
            mockServer.enqueue(new MockResponse()
                    .setBody("{\"marketingPlanId\":\"MP-123\",\"count\":42,\"active\":true}")
                    .setHeader("Content-Type", "application/json"));

            client = createClient();

            MarketingPlanApiClient.EnrichmentResult result = client.enrich(createPayload("ENT-002"));

            assertThat(result.getAdditionalData()).containsEntry("count", 42);
            assertThat(result.getAdditionalData()).containsEntry("active", true);
        }

        @Test
        @DisplayName("should handle missing optional fields")
        void shouldHandleMissingFields() {
            mockServer.enqueue(new MockResponse()
                    .setBody("{\"campaignId\":\"CAMP-789\"}")
                    .setHeader("Content-Type", "application/json"));

            client = createClient();

            MarketingPlanApiClient.EnrichmentResult result = client.enrich(createPayload("ENT-003"));

            assertThat(result.getMarketingPlanId()).isNull();
            assertThat(result.getCampaignId()).isEqualTo("CAMP-789");
        }
    }

    @Nested
    @DisplayName("Client Error Handling (4xx)")
    class ClientErrorHandling {

        @Test
        @DisplayName("should throw non-retryable exception on 400")
        void shouldThrowOnBadRequest() {
            mockServer.enqueue(new MockResponse().setResponseCode(400));

            client = createClient();

            assertThatThrownBy(() -> client.enrich(createPayload("ENT-004")))
                    .isInstanceOf(EnrichmentException.class)
                    .satisfies(e -> {
                        EnrichmentException ex = (EnrichmentException) e;
                        assertThat(ex.getStatusCode()).isEqualTo(400);
                        assertThat(ex.isRetryable()).isFalse();
                    });
        }

        @Test
        @DisplayName("should throw non-retryable exception on 404")
        void shouldThrowOnNotFound() {
            mockServer.enqueue(new MockResponse().setResponseCode(404));

            client = createClient();

            assertThatThrownBy(() -> client.enrich(createPayload("ENT-005")))
                    .isInstanceOf(EnrichmentException.class)
                    .satisfies(e -> {
                        EnrichmentException ex = (EnrichmentException) e;
                        assertThat(ex.getStatusCode()).isEqualTo(404);
                        assertThat(ex.isRetryable()).isFalse();
                    });
        }

        @Test
        @DisplayName("should throw non-retryable exception on 401")
        void shouldThrowOnUnauthorized() {
            mockServer.enqueue(new MockResponse().setResponseCode(401));

            client = createClient();

            assertThatThrownBy(() -> client.enrich(createPayload("ENT-006")))
                    .isInstanceOf(EnrichmentException.class)
                    .satisfies(e -> {
                        EnrichmentException ex = (EnrichmentException) e;
                        assertThat(ex.getStatusCode()).isEqualTo(401);
                        assertThat(ex.isRetryable()).isFalse();
                    });
        }
    }

    @Nested
    @DisplayName("Server Error Handling (5xx)")
    class ServerErrorHandling {

        @Test
        @DisplayName("should throw retryable exception on 500 after retries exhausted")
        void shouldThrowRetryableOn500() {
            // Enqueue enough responses for all retry attempts
            mockServer.enqueue(new MockResponse().setResponseCode(500));
            mockServer.enqueue(new MockResponse().setResponseCode(500));
            mockServer.enqueue(new MockResponse().setResponseCode(500));

            client = createClient();

            assertThatThrownBy(() -> client.enrich(createPayload("ENT-007")))
                    .isInstanceOf(EnrichmentException.class)
                    .satisfies(e -> {
                        EnrichmentException ex = (EnrichmentException) e;
                        assertThat(ex.getStatusCode()).isEqualTo(500);
                        assertThat(ex.isRetryable()).isTrue();
                    });
        }

        @Test
        @DisplayName("should throw retryable exception on 503 after retries exhausted")
        void shouldThrowRetryableOn503() {
            // Enqueue enough responses for all retry attempts
            mockServer.enqueue(new MockResponse().setResponseCode(503));
            mockServer.enqueue(new MockResponse().setResponseCode(503));
            mockServer.enqueue(new MockResponse().setResponseCode(503));

            client = createClient();

            assertThatThrownBy(() -> client.enrich(createPayload("ENT-008")))
                    .isInstanceOf(EnrichmentException.class)
                    .satisfies(e -> {
                        EnrichmentException ex = (EnrichmentException) e;
                        assertThat(ex.getStatusCode()).isEqualTo(503);
                        assertThat(ex.isRetryable()).isTrue();
                    });
        }

        @Test
        @DisplayName("should throw retryable exception on 502 after retries exhausted")
        void shouldThrowRetryableOn502() {
            // Enqueue enough responses for all retry attempts
            mockServer.enqueue(new MockResponse().setResponseCode(502));
            mockServer.enqueue(new MockResponse().setResponseCode(502));
            mockServer.enqueue(new MockResponse().setResponseCode(502));

            client = createClient();

            assertThatThrownBy(() -> client.enrich(createPayload("ENT-009")))
                    .isInstanceOf(EnrichmentException.class)
                    .satisfies(e -> {
                        EnrichmentException ex = (EnrichmentException) e;
                        assertThat(ex.getStatusCode()).isEqualTo(502);
                        assertThat(ex.isRetryable()).isTrue();
                    });
        }
    }

    @Nested
    @DisplayName("Timeout Handling")
    class TimeoutHandling {

        @Test
        @DisplayName("should throw exception on timeout")
        void shouldThrowOnTimeout() {
            OkHttpClient shortTimeoutClient = new OkHttpClient.Builder()
                    .connectTimeout(50, TimeUnit.MILLISECONDS)
                    .readTimeout(50, TimeUnit.MILLISECONDS)
                    .build();

            mockServer.enqueue(new MockResponse()
                    .setHeadersDelay(2, TimeUnit.SECONDS)
                    .setBody("{\"marketingPlanId\":\"MP-123\"}"));

            String baseUrl = mockServer.url("/").toString();
            if (baseUrl.endsWith("/")) {
                baseUrl = baseUrl.substring(0, baseUrl.length() - 1);
            }
            client = new RestMarketingPlanApiClient(
                    jwtTokenProvider,
                    baseUrl,
                    shortTimeoutClient
            );

            assertThatThrownBy(() -> client.enrich(createPayload("ENT-010")))
                    .isInstanceOf(EnrichmentException.class);
        }
    }

    @Nested
    @DisplayName("Response Parsing")
    class ResponseParsing {

        @Test
        @DisplayName("should throw non-retryable on empty body")
        void shouldThrowOnEmptyBody() {
            mockServer.enqueue(new MockResponse()
                    .setResponseCode(200)
                    .setBody(""));

            client = createClient();

            assertThatThrownBy(() -> client.enrich(createPayload("ENT-011")))
                    .isInstanceOf(EnrichmentException.class)
                    .satisfies(e -> {
                        EnrichmentException ex = (EnrichmentException) e;
                        assertThat(ex.isRetryable()).isFalse();
                    });
        }

        @Test
        @DisplayName("should throw non-retryable on invalid JSON")
        void shouldThrowOnInvalidJson() {
            mockServer.enqueue(new MockResponse()
                    .setResponseCode(200)
                    .setBody("not-valid-json"));

            client = createClient();

            assertThatThrownBy(() -> client.enrich(createPayload("ENT-012")))
                    .isInstanceOf(EnrichmentException.class)
                    .satisfies(e -> {
                        EnrichmentException ex = (EnrichmentException) e;
                        assertThat(ex.isRetryable()).isFalse();
                    });
        }
    }

    private RestMarketingPlanApiClient createClient() {
        String baseUrl = mockServer.url("/").toString();
        if (baseUrl.endsWith("/")) {
            baseUrl = baseUrl.substring(0, baseUrl.length() - 1);
        }
        return new RestMarketingPlanApiClient(
                jwtTokenProvider,
                baseUrl,
                httpClient
        );
    }

    private ParsedPayload createPayload(String entityId) {
        return new ParsedPayload(
                "MSG-001",
                "TXN-001",
                "order_created",
                entityId,
                java.util.Map.of("key", "value"),
                java.time.Instant.now(),
                "{}"
        );
    }
}
