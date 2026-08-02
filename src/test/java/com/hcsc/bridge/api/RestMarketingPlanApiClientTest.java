package com.hcsc.bridge.api;

import com.hcsc.bridge.model.ParsedPayload;
import com.hcsc.bridge.security.JwtTokenProvider;
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
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RestMarketingPlanApiClientTest {

    private static final String TEST_CLIENT_ID = "test-client-id";
    private static final String TEST_CLIENT_SECRET = "test-client-secret";

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
        lenient().when(jwtTokenProvider.getToken()).thenReturn("test-jwt-token");
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
                    .setBody(planResponseBody("MP-123"))
                    .setHeader("Content-Type", "application/json"));

            client = createClient();
            ParsedPayload payload = createPayload("ENT-001");

            MarketingPlanApiClient.EnrichmentResult result = client.enrich(payload);

            assertThat(result.getMarketingPlanId()).isEqualTo("MP-123");
            assertThat(result.getCampaignId()).isNull();
            // rawResponse is the unmodified response root whose single child is PlanResponse.
            assertThat(result.getRawResponse().path("PlanResponse")
                    .path("planIdentification").path("marketingPlanIdentifier").asText())
                    .isEqualTo("MP-123");

            RecordedRequest request = mockServer.takeRequest();
            assertThat(request.getMethod()).isEqualTo("GET");
            assertThat(request.getPath()).isEqualTo("/ENT-001/2026-01-01");
            assertThat(request.getHeader("Authorization")).isEqualTo("Bearer test-jwt-token");
            assertThat(request.getHeader("ClientID")).isEqualTo(TEST_CLIENT_ID);
            assertThat(request.getHeader("ClientSecret")).isEqualTo(TEST_CLIENT_SECRET);
        }

        @Test
        @DisplayName("should carry through the entire response as rawResponse")
        void shouldCarryThroughRawResponse() {
            String body = planResponseBody("MP-123");
            mockServer.enqueue(new MockResponse()
                    .setBody(body)
                    .setHeader("Content-Type", "application/json"));

            client = createClient();

            MarketingPlanApiClient.EnrichmentResult result = client.enrich(createPayload("ENT-002"));

            assertThat(result.getRawResponse().path("PlanResponse").path("changeEvent")
                    .path("timestamp").asText()).isEqualTo("20260710T162108.143 CDT");
            assertThat(result.getRawResponse().path("PlanResponse").path("changeEvent")
                    .path("typeName").asText()).isEqualTo("Update");
        }

        @Test
        @DisplayName("should handle missing marketingPlanIdentifier")
        void shouldHandleMissingFields() {
            mockServer.enqueue(new MockResponse()
                    .setBody("{\"PlanResponse\":{\"planIdentification\":{}}}")
                    .setHeader("Content-Type", "application/json"));

            client = createClient();

            MarketingPlanApiClient.EnrichmentResult result = client.enrich(createPayload("ENT-003"));

            assertThat(result.getMarketingPlanId()).isNull();
            assertThat(result.getCampaignId()).isNull();
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
        @DisplayName("should refresh the token once on 401 and fail non-retryably if it repeats")
        void shouldRefreshOnceThenFailOnRepeated401() {
            // A cached-but-revoked token gets exactly one forced refresh; a second 401
            // with the fresh token is a real auth problem and must not loop.
            mockServer.enqueue(new MockResponse().setResponseCode(401));
            mockServer.enqueue(new MockResponse().setResponseCode(401));

            client = createClient();

            assertThatThrownBy(() -> client.enrich(createPayload("ENT-006")))
                    .isInstanceOf(EnrichmentException.class)
                    .satisfies(e -> {
                        EnrichmentException ex = (EnrichmentException) e;
                        assertThat(ex.getStatusCode()).isEqualTo(401);
                        assertThat(ex.isRetryable()).isFalse();
                    });

            org.mockito.Mockito.verify(jwtTokenProvider).refreshToken();
            assertThat(mockServer.getRequestCount()).isEqualTo(2);
        }

        @Test
        @DisplayName("should recover when a 401 is fixed by a token refresh")
        void shouldRecoverAfterTokenRefreshOn401() {
            mockServer.enqueue(new MockResponse().setResponseCode(401));
            mockServer.enqueue(new MockResponse()
                    .setBody(planResponseBody("MP-REFRESHED"))
                    .setHeader("Content-Type", "application/json"));

            client = createClient();

            MarketingPlanApiClient.EnrichmentResult result = client.enrich(createPayload("ENT-006B"));

            assertThat(result.getMarketingPlanId()).isEqualTo("MP-REFRESHED");
            org.mockito.Mockito.verify(jwtTokenProvider).refreshToken();
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
                    TEST_CLIENT_ID,
                    TEST_CLIENT_SECRET,
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

    @Nested
    @DisplayName("Token Failures")
    class TokenFailures {

        @Test
        @DisplayName("should translate token failure into retryable EnrichmentException and retry")
        void shouldTranslateTokenFailure() {
            reset(jwtTokenProvider);
            when(jwtTokenProvider.getToken())
                    .thenThrow(new RuntimeException("Token refresh failed with status: 503"));

            client = createClient();

            assertThatThrownBy(() -> client.enrich(createPayload("ENT-TOK-001")))
                    .isInstanceOf(EnrichmentException.class)
                    .hasMessageContaining("Failed to acquire API token")
                    .satisfies(e -> assertThat(((EnrichmentException) e).isRetryable()).isTrue());

            // Retryable → all attempts consumed, and no HTTP request ever left the client
            verify(jwtTokenProvider, times(3)).getToken();
            assertThat(mockServer.getRequestCount()).isZero();
        }

        @Test
        @DisplayName("should recover when the token succeeds on a later attempt")
        void shouldRecoverOnLaterTokenSuccess() {
            reset(jwtTokenProvider);
            when(jwtTokenProvider.getToken())
                    .thenThrow(new RuntimeException("transient token outage"))
                    .thenReturn("recovered-token");

            mockServer.enqueue(new MockResponse()
                    .setBody(planResponseBody("MP-RECOVERED"))
                    .setHeader("Content-Type", "application/json"));

            client = createClient();

            MarketingPlanApiClient.EnrichmentResult result = client.enrich(createPayload("ENT-TOK-002"));

            assertThat(result.getMarketingPlanId()).isEqualTo("MP-RECOVERED");
        }

        @Test
        @DisplayName("should abort the retry loop when interrupted during backoff")
        void shouldAbortRetryOnInterrupt() {
            mockServer.enqueue(new MockResponse().setResponseCode(500));

            client = createClient();

            Thread.currentThread().interrupt();
            try {
                assertThatThrownBy(() -> client.enrich(createPayload("ENT-INT-001")))
                        .isInstanceOf(EnrichmentException.class)
                        .hasMessageContaining("interrupted")
                        .satisfies(e -> assertThat(((EnrichmentException) e).isRetryable()).isFalse());
            } finally {
                // Reads AND clears the flag so it cannot leak into other tests
                assertThat(Thread.interrupted()).isTrue();
            }
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
                TEST_CLIENT_ID,
                TEST_CLIENT_SECRET,
                httpClient
        );
    }

    private String planResponseBody(String marketingPlanId) {
        return "{\"PlanResponse\":{"
                + "\"planIdentification\":{\"marketingPlanIdentifier\":\"" + marketingPlanId + "\"},"
                + "\"changeEvent\":{\"eventName\":\"ReadyToSell\",\"typeName\":\"Update\","
                + "\"timestamp\":\"20260710T162108.143 CDT\"}"
                + "}}";
    }

    private ParsedPayload createPayload(String entityId) {
        return new ParsedPayload(
                "MSG-001",
                "TXN-001",
                "order_created",
                entityId,
                "2026-01-01",
                java.util.Map.of("key", "value"),
                java.time.Instant.now(),
                "{}"
        );
    }
}
