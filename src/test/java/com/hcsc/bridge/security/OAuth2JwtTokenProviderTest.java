package com.hcsc.bridge.security;

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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OAuth2JwtTokenProviderTest {

    private MockWebServer mockServer;
    private OAuth2JwtTokenProvider tokenProvider;
    private OkHttpClient httpClient;

    @BeforeEach
    void setUp() throws IOException {
        mockServer = new MockWebServer();
        mockServer.start();
        httpClient = new OkHttpClient.Builder()
                .connectTimeout(5, TimeUnit.SECONDS)
                .readTimeout(5, TimeUnit.SECONDS)
                .build();
    }

    @AfterEach
    void tearDown() throws IOException {
        mockServer.shutdown();
    }

    @Nested
    @DisplayName("Token Fetching")
    class TokenFetching {

        @Test
        @DisplayName("should send STS-shaped request: credential headers + JSON body")
        void shouldFetchTokenWithCredentials() throws InterruptedException {
            mockServer.enqueue(new MockResponse()
                    .setBody("{\"access_token\":\"test-jwt-token\",\"expires_in\":3600}")
                    .setHeader("Content-Type", "application/json"));

            tokenProvider = createProvider();

            String token = tokenProvider.getToken();

            assertThat(token).isEqualTo("test-jwt-token");

            RecordedRequest request = mockServer.takeRequest();
            assertThat(request.getMethod()).isEqualTo("POST");
            assertThat(request.getHeader("Content-Type")).startsWith("application/json");
            assertThat(request.getHeader("ClientID")).isEqualTo("test-client");
            assertThat(request.getHeader("ClientSecret")).isEqualTo("test-secret");
            assertThat(request.getHeader("scope")).isEqualTo("api.read");
            String body = request.getBody().readUtf8();
            assertThat(body).contains("\"username\":\"test-user\"");
            assertThat(body).contains("\"password\":\"test-password\"");
            assertThat(body).doesNotContain("client_id").doesNotContain("client_secret");
        }

        @Test
        @DisplayName("should cache token and not re-fetch")
        void shouldCacheToken() {
            mockServer.enqueue(new MockResponse()
                    .setBody("{\"access_token\":\"cached-token\",\"expires_in\":3600}")
                    .setHeader("Content-Type", "application/json"));

            tokenProvider = createProvider();

            String firstCall = tokenProvider.getToken();
            String secondCall = tokenProvider.getToken();
            String thirdCall = tokenProvider.getToken();

            assertThat(firstCall).isEqualTo("cached-token");
            assertThat(secondCall).isEqualTo("cached-token");
            assertThat(thirdCall).isEqualTo("cached-token");
            assertThat(mockServer.getRequestCount()).isEqualTo(1);
        }

        @Test
        @DisplayName("should handle missing expires_in with default")
        void shouldHandleMissingExpiresIn() {
            mockServer.enqueue(new MockResponse()
                    .setBody("{\"access_token\":\"token-no-expiry\"}")
                    .setHeader("Content-Type", "application/json"));

            tokenProvider = createProvider();

            String token = tokenProvider.getToken();

            assertThat(token).isEqualTo("token-no-expiry");
            assertThat(tokenProvider.isTokenValid()).isTrue();
        }
    }

    @Nested
    @DisplayName("Token Refresh")
    class TokenRefresh {

        @Test
        @DisplayName("should refresh token on explicit refresh call")
        void shouldRefreshExplicitly() {
            mockServer.enqueue(new MockResponse()
                    .setBody("{\"access_token\":\"token-v1\",\"expires_in\":3600}")
                    .setHeader("Content-Type", "application/json"));
            mockServer.enqueue(new MockResponse()
                    .setBody("{\"access_token\":\"token-v2\",\"expires_in\":3600}")
                    .setHeader("Content-Type", "application/json"));

            tokenProvider = createProvider();

            String firstToken = tokenProvider.getToken();
            tokenProvider.refreshToken();
            String secondToken = tokenProvider.getToken();

            assertThat(firstToken).isEqualTo("token-v1");
            assertThat(secondToken).isEqualTo("token-v2");
            assertThat(mockServer.getRequestCount()).isEqualTo(2);
        }

        @Test
        @DisplayName("should check token validity")
        void shouldCheckValidity() {
            mockServer.enqueue(new MockResponse()
                    .setBody("{\"access_token\":\"valid-token\",\"expires_in\":3600}")
                    .setHeader("Content-Type", "application/json"));

            tokenProvider = createProvider();
            assertThat(tokenProvider.isTokenValid()).isFalse();

            tokenProvider.getToken();
            assertThat(tokenProvider.isTokenValid()).isTrue();
        }
    }

    @Nested
    @DisplayName("Error Handling")
    class ErrorHandling {

        @Test
        @DisplayName("should throw exception when token URL not configured")
        void shouldThrowWhenNoTokenUrl() {
            tokenProvider = new OAuth2JwtTokenProvider(
                    "", "client", "secret", "scope", "user", "pass", httpClient);

            assertThatThrownBy(() -> tokenProvider.getToken())
                    .isInstanceOf(OAuth2JwtTokenProvider.TokenRefreshException.class)
                    .hasMessageContaining("Token URL not configured");
        }

        @Test
        @DisplayName("should throw exception on HTTP error")
        void shouldThrowOnHttpError() {
            mockServer.enqueue(new MockResponse().setResponseCode(401));

            tokenProvider = createProvider();

            assertThatThrownBy(() -> tokenProvider.getToken())
                    .isInstanceOf(OAuth2JwtTokenProvider.TokenRefreshException.class)
                    .hasMessageContaining("failed with status: 401");
        }

        @Test
        @DisplayName("should throw exception when response has no recognized token field")
        void shouldThrowWhenMissingAccessToken() {
            mockServer.enqueue(new MockResponse()
                    .setBody("{\"error\":\"invalid_grant\"}")
                    .setHeader("Content-Type", "application/json"));

            tokenProvider = createProvider();

            assertThatThrownBy(() -> tokenProvider.getToken())
                    .isInstanceOf(OAuth2JwtTokenProvider.TokenRefreshException.class)
                    .hasMessageContaining("no recognized token field")
                    .hasMessageContaining("error");
        }

        @Test
        @DisplayName("should accept alternative token field names")
        void shouldAcceptAlternativeTokenFieldNames() {
            mockServer.enqueue(new MockResponse()
                    .setBody("{\"token\":\"alt-field-token\"}")
                    .setHeader("Content-Type", "application/json"));

            tokenProvider = createProvider();

            assertThat(tokenProvider.getToken()).isEqualTo("alt-field-token");
        }

        @Test
        @DisplayName("should read expiry from the JWT exp claim when no expires field")
        void shouldReadExpiryFromJwtExpClaim() {
            // header {"alg":"none"} . payload {"exp": 4102444800} (year 2100) . empty sig
            String jwt = java.util.Base64.getUrlEncoder().withoutPadding()
                    .encodeToString("{\"alg\":\"none\"}".getBytes())
                    + "."
                    + java.util.Base64.getUrlEncoder().withoutPadding()
                    .encodeToString("{\"exp\":4102444800}".getBytes())
                    + ".";
            mockServer.enqueue(new MockResponse()
                    .setBody("{\"token\":\"" + jwt + "\"}")
                    .setHeader("Content-Type", "application/json"));

            tokenProvider = createProvider();

            assertThat(tokenProvider.getToken()).isEqualTo(jwt);
            assertThat(tokenProvider.isTokenValid()).isTrue();
        }

        @Test
        @DisplayName("should throw exception on server error")
        void shouldThrowOnServerError() {
            mockServer.enqueue(new MockResponse().setResponseCode(500));

            tokenProvider = createProvider();

            assertThatThrownBy(() -> tokenProvider.getToken())
                    .isInstanceOf(OAuth2JwtTokenProvider.TokenRefreshException.class)
                    .hasMessageContaining("failed with status: 500");
        }
    }

    @Nested
    @DisplayName("Thread Safety")
    class ThreadSafety {

        @Test
        @DisplayName("should handle concurrent token requests")
        void shouldHandleConcurrentRequests() throws InterruptedException {
            mockServer.enqueue(new MockResponse()
                    .setBody("{\"access_token\":\"concurrent-token\",\"expires_in\":3600}")
                    .setHeader("Content-Type", "application/json"));

            tokenProvider = createProvider();

            int threadCount = 10;
            ExecutorService executor = Executors.newFixedThreadPool(threadCount);
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch completeLatch = new CountDownLatch(threadCount);
            AtomicInteger successCount = new AtomicInteger(0);

            for (int i = 0; i < threadCount; i++) {
                executor.submit(() -> {
                    try {
                        startLatch.await();
                        String token = tokenProvider.getToken();
                        if ("concurrent-token".equals(token)) {
                            successCount.incrementAndGet();
                        }
                    } catch (Exception e) {
                        // Ignore
                    } finally {
                        completeLatch.countDown();
                    }
                });
            }

            startLatch.countDown();
            completeLatch.await(10, TimeUnit.SECONDS);
            executor.shutdown();

            assertThat(successCount.get()).isEqualTo(threadCount);
            assertThat(mockServer.getRequestCount()).isEqualTo(1);
        }
    }

    @Nested
    @DisplayName("Optional Fields")
    class OptionalFields {

        @Test
        @DisplayName("should omit scope when not configured")
        void shouldOmitScopeWhenEmpty() throws InterruptedException {
            mockServer.enqueue(new MockResponse()
                    .setBody("{\"access_token\":\"no-scope-token\",\"expires_in\":3600}")
                    .setHeader("Content-Type", "application/json"));

            tokenProvider = new OAuth2JwtTokenProvider(
                    mockServer.url("/token").toString(),
                    "client",
                    "secret",
                    "",
                    "user",
                    "pass",
                    httpClient
            );

            tokenProvider.getToken();

            RecordedRequest request = mockServer.takeRequest();
            assertThat(request.getHeader("scope")).isNull();
        }

        @Test
        @DisplayName("should omit username when not configured")
        void shouldOmitUsernameWhenEmpty() throws InterruptedException {
            mockServer.enqueue(new MockResponse()
                    .setBody("{\"access_token\":\"no-user-token\",\"expires_in\":3600}")
                    .setHeader("Content-Type", "application/json"));

            tokenProvider = new OAuth2JwtTokenProvider(
                    mockServer.url("/token").toString(),
                    "client",
                    "secret",
                    "scope",
                    "",
                    "",
                    httpClient
            );

            tokenProvider.getToken();

            RecordedRequest request = mockServer.takeRequest();
            String body = request.getBody().readUtf8();
            assertThat(body).doesNotContain("username");
            assertThat(body).doesNotContain("password");
        }

        @Test
        @DisplayName("should include all credentials when configured")
        void shouldIncludeAllCredentials() throws InterruptedException {
            mockServer.enqueue(new MockResponse()
                    .setBody("{\"access_token\":\"full-token\",\"expires_in\":3600}")
                    .setHeader("Content-Type", "application/json"));

            tokenProvider = createProvider();
            tokenProvider.getToken();

            RecordedRequest request = mockServer.takeRequest();
            assertThat(request.getHeader("ClientID")).isEqualTo("test-client");
            assertThat(request.getHeader("ClientSecret")).isEqualTo("test-secret");
            assertThat(request.getHeader("scope")).isEqualTo("api.read");
            String body = request.getBody().readUtf8();
            assertThat(body).contains("\"username\":\"test-user\"");
            assertThat(body).contains("\"password\":\"test-password\"");
        }
    }

    private OAuth2JwtTokenProvider createProvider() {
        return new OAuth2JwtTokenProvider(
                mockServer.url("/token").toString(),
                "test-client",
                "test-secret",
                "api.read",
                "test-user",
                "test-password",
                httpClient
        );
    }
}
