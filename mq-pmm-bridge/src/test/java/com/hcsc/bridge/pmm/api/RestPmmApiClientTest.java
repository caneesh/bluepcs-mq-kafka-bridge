package com.hcsc.bridge.pmm.api;

import com.hcsc.bridge.mock.MockJwtTokenProvider;
import com.hcsc.bridge.security.JwtTokenProvider;
import okhttp3.OkHttpClient;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import okhttp3.mockwebserver.SocketPolicy;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@DisplayName("RestPmmApiClient")
class RestPmmApiClientTest {

    private static final String REQUEST = "<PmmRequest><Id>1</Id></PmmRequest>";
    private static final String RESPONSE = "<PmmResponse><Status>OK</Status><At>2026-09-13T10:00:00Z</At></PmmResponse>";

    private MockWebServer server;
    private MockJwtTokenProvider tokenProvider;
    private RestPmmApiClient client;

    @BeforeEach
    void setUp() throws IOException {
        server = new MockWebServer();
        server.start();
        tokenProvider = new MockJwtTokenProvider();
        tokenProvider.setToken("tok-1");
        client = newClient(tokenProvider, 3);
    }

    private RestPmmApiClient newClient(JwtTokenProvider provider, int attempts) {
        OkHttpClient http = new OkHttpClient.Builder()
                .connectTimeout(2, TimeUnit.SECONDS)
                .readTimeout(1, TimeUnit.SECONDS)
                .build();
        return new RestPmmApiClient(provider, server.url("/pmm").toString(), "cid", "csecret", http,
                attempts, 10, "application/xml", "application/xml");
    }

    @AfterEach
    void tearDown() throws IOException {
        server.shutdown();
    }

    @Nested
    @DisplayName("successful call")
    class Success {

        @Test
        @DisplayName("POSTs the body with the exact headers and returns the raw response")
        void postsWithHeaders() throws Exception {
            server.enqueue(new MockResponse().setBody(RESPONSE).setHeader("Content-Type", "application/xml"));

            PmmApiResponse response = client.submit(REQUEST, "evt-1");

            assertThat(response.getStatusCode()).isEqualTo(200);
            assertThat(response.getBody()).isEqualTo(RESPONSE);
            RecordedRequest recorded = server.takeRequest();
            assertThat(recorded.getMethod()).isEqualTo("POST");
            assertThat(recorded.getPath()).isEqualTo("/pmm");
            assertThat(recorded.getBody().readUtf8()).isEqualTo(REQUEST);
            assertThat(recorded.getHeader("Authorization")).isEqualTo("Bearer tok-1");
            assertThat(recorded.getHeader("Content-Type")).isEqualTo("application/xml");
            assertThat(recorded.getHeader("Accept")).isEqualTo("application/xml");
            assertThat(recorded.getHeader("ClientID")).isEqualTo("cid");
            assertThat(recorded.getHeader("ClientSecret")).isEqualTo("csecret");
        }
    }

    @Nested
    @DisplayName("auth handling")
    class Auth {

        @Test
        @DisplayName("refreshes the token exactly once on 401 and retries")
        void refreshesOnceOn401() throws Exception {
            server.enqueue(new MockResponse().setResponseCode(401));
            server.enqueue(new MockResponse().setBody(RESPONSE));

            PmmApiResponse response = client.submit(REQUEST, "evt-1");

            assertThat(response.getBody()).isEqualTo(RESPONSE);
            assertThat(tokenProvider.getRefreshCount()).isEqualTo(1);
            assertThat(server.getRequestCount()).isEqualTo(2);
        }

        @Test
        @DisplayName("a persisting 403 after the refresh is retryable, not permanent")
        void persisting403IsRetryable() {
            for (int i = 0; i < 4; i++) {
                server.enqueue(new MockResponse().setResponseCode(403));
            }

            assertThatThrownBy(() -> client.submit(REQUEST, "evt-1"))
                    .isInstanceOf(PmmApiException.class)
                    .satisfies(e -> assertThat(((PmmApiException) e).isRetryable()).isTrue());
            assertThat(tokenProvider.getRefreshCount()).isEqualTo(1);
        }

        @Test
        @DisplayName("token acquisition failure is a retryable exception, not a bare RuntimeException")
        void tokenAcquisitionFailure() {
            JwtTokenProvider failing = mock(JwtTokenProvider.class);
            when(failing.getToken()).thenThrow(new IllegalStateException("STS down"));

            assertThatThrownBy(() -> newClient(failing, 1).submit(REQUEST, "evt-1"))
                    .isInstanceOf(PmmApiException.class)
                    .hasMessageContaining("STS down")
                    .satisfies(e -> assertThat(((PmmApiException) e).isRetryable()).isTrue());
            assertThat(server.getRequestCount()).isZero();
        }

        @Test
        @DisplayName("a failing refresh after 401 is retryable")
        void refreshFailure() {
            JwtTokenProvider failing = mock(JwtTokenProvider.class);
            when(failing.getToken()).thenReturn("tok");
            doThrow(new IllegalStateException("refresh down")).when(failing).refreshToken();
            server.enqueue(new MockResponse().setResponseCode(401));

            assertThatThrownBy(() -> newClient(failing, 3).submit(REQUEST, "evt-1"))
                    .isInstanceOf(PmmApiException.class)
                    .hasMessageContaining("refresh")
                    .satisfies(e -> assertThat(((PmmApiException) e).isRetryable()).isTrue());
        }
    }

    @Nested
    @DisplayName("retryability")
    class Retryability {

        @Test
        @DisplayName("retries 5xx up to retry-attempts then throws retryable")
        void serverErrors() {
            for (int i = 0; i < 3; i++) {
                server.enqueue(new MockResponse().setResponseCode(503).setBody("<e>busy</e>"));
            }

            assertThatThrownBy(() -> client.submit(REQUEST, "evt-1"))
                    .isInstanceOf(PmmApiException.class)
                    .satisfies(e -> {
                        assertThat(((PmmApiException) e).isRetryable()).isTrue();
                        assertThat(((PmmApiException) e).getStatusCode()).isEqualTo(503);
                    });
            assertThat(server.getRequestCount()).isEqualTo(3);
        }

        @Test
        @DisplayName("treats 404 as permanent after a single attempt")
        void notFoundIsPermanent() {
            server.enqueue(new MockResponse().setResponseCode(404));

            assertThatThrownBy(() -> client.submit(REQUEST, "evt-1"))
                    .isInstanceOf(PmmApiException.class)
                    .satisfies(e -> assertThat(((PmmApiException) e).isRetryable()).isFalse());
            assertThat(server.getRequestCount()).isEqualTo(1);
        }

        @Test
        @DisplayName("treats 429 and 408 as retryable")
        void throttleAndTimeoutStatuses() {
            server.enqueue(new MockResponse().setResponseCode(429));
            server.enqueue(new MockResponse().setResponseCode(408));
            server.enqueue(new MockResponse().setBody(RESPONSE));

            assertThat(client.submit(REQUEST, "evt-1").getBody()).isEqualTo(RESPONSE);
            assertThat(server.getRequestCount()).isEqualTo(3);
        }

        @Test
        @DisplayName("an empty 2xx body is permanent")
        void emptyBody() {
            server.enqueue(new MockResponse().setResponseCode(200));

            assertThatThrownBy(() -> client.submit(REQUEST, "evt-1"))
                    .isInstanceOf(PmmApiException.class)
                    .hasMessageContaining("Empty")
                    .satisfies(e -> assertThat(((PmmApiException) e).isRetryable()).isFalse());
        }

        @Test
        @DisplayName("a read timeout is retryable")
        void timeout() {
            server.enqueue(new MockResponse().setSocketPolicy(SocketPolicy.NO_RESPONSE));

            assertThatThrownBy(() -> newClient(tokenProvider, 1).submit(REQUEST, "evt-1"))
                    .isInstanceOf(PmmApiException.class)
                    .hasMessageContaining("timed out")
                    .satisfies(e -> assertThat(((PmmApiException) e).isRetryable()).isTrue());
        }
    }
}
