package com.hcsc.bridge.config;

import com.hcsc.bridge.config.ReadinessCheckService.CheckResult;
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

@DisplayName("ReadinessCheckService OAuth token check")
class ReadinessCheckOAuthTest {

    private MockWebServer sts;
    private ReadinessCheckService service;

    @BeforeEach
    void setUp() throws IOException {
        sts = new MockWebServer();
        sts.start();
        service = new ReadinessCheckService();
        // Everything else unconfigured so the other checks SKIP
        ReflectionTestUtils.setField(service, "mqHost", "");
        ReflectionTestUtils.setField(service, "kafkaBootstrapServers", "");
        ReflectionTestUtils.setField(service, "hdfsNamenode", "");
        ReflectionTestUtils.setField(service, "oauthTokenUrl", sts.url("/sts/token").toString());
        ReflectionTestUtils.setField(service, "oauthClientId", "cid");
        ReflectionTestUtils.setField(service, "oauthClientSecret", "csecret");
        ReflectionTestUtils.setField(service, "oauthScope", "openid");
        ReflectionTestUtils.setField(service, "oauthUsername", "user");
        ReflectionTestUtils.setField(service, "oauthPassword", "pass");
    }

    @AfterEach
    void tearDown() throws IOException {
        sts.shutdown();
    }

    private CheckResult oauthResult() {
        return service.runAllChecks().getResults().stream()
                .filter(r -> "OAUTH_TOKEN".equals(r.getName()))
                .findFirst().orElseThrow();
    }

    @Test
    @DisplayName("passes only when the 2xx body carries a token the runtime provider would accept")
    void passesWithToken() throws Exception {
        sts.enqueue(new MockResponse().setBody("{\"jwt_token\":\"Bearer eyJ.a.b\",\"expires_in\":3600}")
                .setHeader("Content-Type", "application/json"));

        CheckResult result = oauthResult();

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
    void failsOnEmptyJsonObject() {
        sts.enqueue(new MockResponse().setBody("{\"error\":\"none\"}").setHeader("Content-Type", "application/json"));

        CheckResult result = oauthResult();

        assertThat(result.isFailed()).isTrue();
        assertThat(result.getMessage()).contains("no recognized token field").contains("error");
    }

    @Test
    @DisplayName("fails on a 2xx with an empty or non-JSON body")
    void failsOnNonJsonBody() {
        sts.enqueue(new MockResponse().setBody("<html>login</html>").setHeader("Content-Type", "text/html"));

        CheckResult result = oauthResult();

        assertThat(result.isFailed()).isTrue();
        assertThat(result.getMessage()).contains("no recognized token field").contains("unparseable");
    }

    @Test
    @DisplayName("fails on a non-2xx status")
    void failsOnRejectedCredentials() {
        sts.enqueue(new MockResponse().setResponseCode(401).setBody("{\"error\":\"invalid_client\"}"));

        assertThat(oauthResult().isFailed()).isTrue();
    }
}
