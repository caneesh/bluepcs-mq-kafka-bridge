package com.enterprise.bridge.integration;

import com.enterprise.bridge.api.EnrichmentException;
import com.enterprise.bridge.api.MarketingPlanApiClient;
import com.enterprise.bridge.api.RestMarketingPlanApiClient;
import com.enterprise.bridge.mock.MockJwtTokenProvider;
import com.enterprise.bridge.model.ParsedPayload;
import okhttp3.OkHttpClient;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RestApiClientIT {

    private MockWebServer mockWebServer;
    private RestMarketingPlanApiClient apiClient;
    private MockJwtTokenProvider jwtTokenProvider;
    private OkHttpClient httpClient;

    @BeforeEach
    void setUp() throws IOException {
        mockWebServer = new MockWebServer();
        mockWebServer.start();

        httpClient = new OkHttpClient.Builder()
                .connectTimeout(5, TimeUnit.SECONDS)
                .readTimeout(5, TimeUnit.SECONDS)
                .build();
        jwtTokenProvider = new MockJwtTokenProvider();
        String baseUrl = mockWebServer.url("/").toString();
        baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;

        apiClient = new RestMarketingPlanApiClient(jwtTokenProvider, baseUrl, httpClient);
    }

    @AfterEach
    void tearDown() throws IOException {
        mockWebServer.shutdown();
    }

    @Nested
    @DisplayName("Successful Enrichment Tests")
    class SuccessfulEnrichment {

        @Test
        @DisplayName("should return enrichment result on successful response")
        void shouldReturnEnrichmentResult() throws Exception {
            String responseBody = "{\"marketingPlanId\":\"MP-12345\",\"campaignId\":\"CAMP-67890\",\"segment\":\"premium\",\"tier\":\"gold\"}";
            mockWebServer.enqueue(new MockResponse()
                    .setBody(responseBody)
                    .setHeader("Content-Type", "application/json"));

            ParsedPayload payload = createTestPayload("MSG-001", "ENTITY-001");

            MarketingPlanApiClient.EnrichmentResult result = apiClient.enrich(payload);

            assertThat(result).isNotNull();
            assertThat(result.getMarketingPlanId()).isEqualTo("MP-12345");
            assertThat(result.getCampaignId()).isEqualTo("CAMP-67890");
            assertThat(result.getAdditionalData()).containsEntry("segment", "premium");
            assertThat(result.getAdditionalData()).containsEntry("tier", "gold");
        }

        @Test
        @DisplayName("should send correct authorization header")
        void shouldSendCorrectAuthHeader() throws Exception {
            jwtTokenProvider.setToken("test-jwt-token-123");
            mockWebServer.enqueue(new MockResponse()
                    .setBody("{\"marketingPlanId\":\"MP-1\",\"campaignId\":\"CAMP-1\"}")
                    .setHeader("Content-Type", "application/json"));

            ParsedPayload payload = createTestPayload("MSG-002", "ENTITY-002");
            apiClient.enrich(payload);

            RecordedRequest request = mockWebServer.takeRequest(5, TimeUnit.SECONDS);
            assertThat(request).isNotNull();
            assertThat(request.getHeader("Authorization")).isEqualTo("Bearer test-jwt-token-123");
        }

        @Test
        @DisplayName("should call correct endpoint URL")
        void shouldCallCorrectEndpoint() throws Exception {
            mockWebServer.enqueue(new MockResponse()
                    .setBody("{\"marketingPlanId\":\"MP-1\",\"campaignId\":\"CAMP-1\"}")
                    .setHeader("Content-Type", "application/json"));

            ParsedPayload payload = createTestPayload("MSG-003", "ENTITY-003");
            apiClient.enrich(payload);

            RecordedRequest request = mockWebServer.takeRequest(5, TimeUnit.SECONDS);
            assertThat(request).isNotNull();
            assertThat(request.getPath()).isEqualTo("/api/v1/marketing-plans/ENTITY-003");
            assertThat(request.getMethod()).isEqualTo("GET");
        }
    }

    @Nested
    @DisplayName("HTTP 500 Error Tests")
    class Http500ErrorTests {

        @Test
        @DisplayName("should throw EnrichmentException on 500 error")
        void shouldThrowOnServerError() {
            mockWebServer.enqueue(new MockResponse()
                    .setResponseCode(500)
                    .setBody("Internal Server Error"));

            ParsedPayload payload = createTestPayload("MSG-500-001", "ENTITY-500-001");

            assertThatThrownBy(() -> apiClient.enrich(payload))
                    .isInstanceOf(EnrichmentException.class)
                    .hasMessageContaining("500");
        }

        @Test
        @DisplayName("should include entity ID in exception on 500")
        void shouldIncludeEntityIdInException() {
            mockWebServer.enqueue(new MockResponse()
                    .setResponseCode(500));

            ParsedPayload payload = createTestPayload("MSG-500-002", "ENTITY-500-002");

            assertThatThrownBy(() -> apiClient.enrich(payload))
                    .isInstanceOf(EnrichmentException.class)
                    .extracting("entityId")
                    .isEqualTo("ENTITY-500-002");
        }

        @Test
        @DisplayName("should include status code in exception on 500")
        void shouldIncludeStatusCodeInException() {
            mockWebServer.enqueue(new MockResponse()
                    .setResponseCode(500));

            ParsedPayload payload = createTestPayload("MSG-500-003", "ENTITY-500-003");

            assertThatThrownBy(() -> apiClient.enrich(payload))
                    .isInstanceOf(EnrichmentException.class)
                    .extracting("statusCode")
                    .isEqualTo(500);
        }

        @Test
        @DisplayName("should be retryable on 500")
        void shouldBeRetryableOn500() {
            mockWebServer.enqueue(new MockResponse().setResponseCode(500));

            ParsedPayload payload = createTestPayload("MSG-500-004", "ENTITY-500-004");

            assertThatThrownBy(() -> apiClient.enrich(payload))
                    .isInstanceOf(EnrichmentException.class)
                    .satisfies(e -> assertThat(((EnrichmentException) e).isRetryable()).isTrue());
        }
    }

    @Nested
    @DisplayName("Timeout Handling Tests")
    class TimeoutHandlingTests {

        @Test
        @DisplayName("should throw EnrichmentException on connection timeout")
        void shouldThrowOnTimeout() throws IOException {
            MockWebServer slowServer = new MockWebServer();
            slowServer.start();
            try {
                slowServer.enqueue(new MockResponse()
                        .setBodyDelay(5, TimeUnit.SECONDS)
                        .setBody("{\"marketingPlanId\":\"MP-1\"}"));

                OkHttpClient shortTimeoutClient = new OkHttpClient.Builder()
                        .connectTimeout(100, TimeUnit.MILLISECONDS)
                        .readTimeout(100, TimeUnit.MILLISECONDS)
                        .build();

                String baseUrl = slowServer.url("/").toString();
                baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
                RestMarketingPlanApiClient timeoutClient = new RestMarketingPlanApiClient(
                        jwtTokenProvider, baseUrl, shortTimeoutClient);

                ParsedPayload payload = createTestPayload("MSG-TIMEOUT-001", "ENTITY-TIMEOUT-001");

                assertThatThrownBy(() -> timeoutClient.enrich(payload))
                        .isInstanceOf(EnrichmentException.class)
                        .satisfies(e -> assertThat(((EnrichmentException) e).isRetryable()).isTrue());
            } finally {
                slowServer.shutdown();
            }
        }
    }

    @Nested
    @DisplayName("Invalid JSON Response Tests")
    class InvalidJsonResponseTests {

        @Test
        @DisplayName("should throw EnrichmentException on invalid JSON")
        void shouldThrowOnInvalidJson() {
            mockWebServer.enqueue(new MockResponse()
                    .setBody("{ invalid json }")
                    .setHeader("Content-Type", "application/json"));

            ParsedPayload payload = createTestPayload("MSG-INVALID-001", "ENTITY-INVALID-001");

            assertThatThrownBy(() -> apiClient.enrich(payload))
                    .isInstanceOf(EnrichmentException.class);
        }

        @Test
        @DisplayName("should throw EnrichmentException on empty response")
        void shouldThrowOnEmptyResponse() {
            mockWebServer.enqueue(new MockResponse()
                    .setBody("")
                    .setHeader("Content-Type", "application/json"));

            ParsedPayload payload = createTestPayload("MSG-EMPTY-001", "ENTITY-EMPTY-001");

            assertThatThrownBy(() -> apiClient.enrich(payload))
                    .isInstanceOf(EnrichmentException.class);
        }

        @Test
        @DisplayName("should handle null fields in response")
        void shouldHandleNullFields() throws Exception {
            mockWebServer.enqueue(new MockResponse()
                    .setBody("{\"marketingPlanId\":null,\"campaignId\":null}")
                    .setHeader("Content-Type", "application/json"));

            ParsedPayload payload = createTestPayload("MSG-NULL-001", "ENTITY-NULL-001");

            MarketingPlanApiClient.EnrichmentResult result = apiClient.enrich(payload);

            assertThat(result).isNotNull();
            assertThat(result.getMarketingPlanId()).isEqualTo("null");
            assertThat(result.getCampaignId()).isEqualTo("null");
        }
    }

    @Nested
    @DisplayName("HTTP 4xx Error Tests")
    class Http4xxErrorTests {

        @Test
        @DisplayName("should throw EnrichmentException on 404")
        void shouldThrowOn404() {
            mockWebServer.enqueue(new MockResponse()
                    .setResponseCode(404)
                    .setBody("Not Found"));

            ParsedPayload payload = createTestPayload("MSG-404-001", "ENTITY-404-001");

            assertThatThrownBy(() -> apiClient.enrich(payload))
                    .isInstanceOf(EnrichmentException.class)
                    .extracting("statusCode")
                    .isEqualTo(404);
        }

        @Test
        @DisplayName("should throw EnrichmentException on 401")
        void shouldThrowOn401() {
            mockWebServer.enqueue(new MockResponse()
                    .setResponseCode(401)
                    .setBody("Unauthorized"));

            ParsedPayload payload = createTestPayload("MSG-401-001", "ENTITY-401-001");

            assertThatThrownBy(() -> apiClient.enrich(payload))
                    .isInstanceOf(EnrichmentException.class)
                    .extracting("statusCode")
                    .isEqualTo(401);
        }

        @Test
        @DisplayName("should not be retryable on 4xx")
        void shouldNotBeRetryableOn4xx() {
            mockWebServer.enqueue(new MockResponse().setResponseCode(400));

            ParsedPayload payload = createTestPayload("MSG-400-001", "ENTITY-400-001");

            assertThatThrownBy(() -> apiClient.enrich(payload))
                    .isInstanceOf(EnrichmentException.class)
                    .satisfies(e -> assertThat(((EnrichmentException) e).isRetryable()).isFalse());
        }
    }

    private ParsedPayload createTestPayload(String messageId, String entityId) {
        return new ParsedPayload(
                messageId,
                "TXN-" + messageId,
                "order_created",
                entityId,
                Map.of("orderId", "ORD-001", "amount", 100.0),
                Instant.now(),
                "{\"test\":\"payload\"}"
        );
    }
}
