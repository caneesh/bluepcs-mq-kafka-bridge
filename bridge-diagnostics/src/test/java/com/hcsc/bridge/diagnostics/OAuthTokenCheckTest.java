package com.hcsc.bridge.diagnostics;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("OAuthTokenCheck")
class OAuthTokenCheckTest {

    private MockWebServer sts;
    private OAuthTokenCheck check;

    @BeforeEach
    void setUp() throws IOException {
        sts = new MockWebServer();
        sts.start();
        check = new OAuthTokenCheck();
        ReflectionTestUtils.setField(check, "oauthTokenUrl", sts.url("/sts/token").toString());
        ReflectionTestUtils.setField(check, "oauthClientId", "cid");
        ReflectionTestUtils.setField(check, "oauthClientSecret", "csecret");
        ReflectionTestUtils.setField(check, "oauthScope", "openid");
        ReflectionTestUtils.setField(check, "oauthUsername", "user");
        ReflectionTestUtils.setField(check, "oauthPassword", "pass");
    }

    @AfterEach
    void tearDown() throws IOException {
        sts.shutdown();
    }

    @Test
    @DisplayName("passes only when the 2xx body carries a token the runtime provider would accept")
    void passesWithToken() throws Exception {
        sts.enqueue(new MockResponse().setBody("{\"jwt_token\":\"Bearer eyJ.a.b\",\"expires_in\":3600}")
                .setHeader("Content-Type", "application/json"));

        CheckResult result = check.run();

        assertThat(result.isPassed()).isTrue();
        assertThat(result.getMessage()).contains("acquired");
        RecordedRequest request = sts.takeRequest();
        assertThat(request.getHeader("ClientID")).isEqualTo("cid");
        assertThat(request.getHeader("ClientSecret")).isEqualTo("csecret");
        assertThat(request.getHeader("scope")).isEqualTo("openid");
        assertThat(request.getBody().readUtf8()).contains("\"username\":\"user\"");
    }

    @Test
    @DisplayName("fails on a 2xx whose body has no token field, naming the fields it did carry")
    void failsOnTokenlessJson() {
        sts.enqueue(new MockResponse().setBody("{\"error\":\"none\"}").setHeader("Content-Type", "application/json"));

        CheckResult result = check.run();

        assertThat(result.isFailed()).isTrue();
        assertThat(result.getMessage()).contains("no recognized token field").contains("error");
    }

    @Test
    @DisplayName("fails on a 2xx with a non-JSON body")
    void failsOnNonJsonBody() {
        sts.enqueue(new MockResponse().setBody("<html>login</html>").setHeader("Content-Type", "text/html"));

        CheckResult result = check.run();

        assertThat(result.isFailed()).isTrue();
        assertThat(result.getMessage()).contains("no recognized token field").contains("unparseable");
    }

    @Test
    @DisplayName("fails on a non-2xx status")
    void failsOnRejectedCredentials() {
        sts.enqueue(new MockResponse().setResponseCode(401).setBody("{\"error\":\"invalid_client\"}"));

        assertThat(check.run().isFailed()).isTrue();
    }

    @Test
    @DisplayName("skips when the STS is not configured, so a partial environment is not a failure")
    void skipsWhenUnconfigured() {
        ReflectionTestUtils.setField(check, "oauthTokenUrl", "");

        CheckResult result = check.run();

        assertThat(result.isSkipped()).isTrue();
        assertThat(result.getName()).isEqualTo("OAUTH_TOKEN");
    }
}
