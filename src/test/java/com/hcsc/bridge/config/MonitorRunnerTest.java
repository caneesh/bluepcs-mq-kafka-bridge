package com.hcsc.bridge.config;

import com.hcsc.bridge.hdfs.HdfsFileOperations;
import com.hcsc.bridge.hdfs.HdfsFileOperations.HdfsFileInfo;
import okhttp3.OkHttpClient;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.context.ApplicationContext;
import org.springframework.core.env.Environment;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@DisplayName("MonitorRunner")
class MonitorRunnerTest {

    private static final String LANDING_PATH = "/data/bridge/payloads";

    private MockWebServer mockServer;
    private MonitorRunner runner;

    @Mock
    private HdfsFileOperations hdfsFileOperations;
    @Mock
    private ApplicationContext applicationContext;
    @Mock
    private Environment environment;

    @BeforeEach
    void setUp() throws IOException {
        mockServer = new MockWebServer();
        mockServer.start();

        OkHttpClient httpClient = new OkHttpClient.Builder()
                .connectTimeout(2, TimeUnit.SECONDS)
                .readTimeout(2, TimeUnit.SECONDS)
                .build();
        runner = new MonitorRunner(hdfsFileOperations, applicationContext, environment, httpClient);
        ReflectionTestUtils.setField(runner, "healthUrl", mockServer.url("/actuator/health").toString());
        ReflectionTestUtils.setField(runner, "backlogAgeMinutes", 30L);
        ReflectionTestUtils.setField(runner, "backlogMaxFiles", 0);
        ReflectionTestUtils.setField(runner, "landingPath", LANDING_PATH);
    }

    @AfterEach
    void tearDown() throws IOException {
        mockServer.shutdown();
    }

    private void enqueueHealth(String body) {
        mockServer.enqueue(new MockResponse()
                .setBody(body)
                .setHeader("Content-Type", "application/json"));
    }

    private HdfsFileInfo freshFile() {
        return new HdfsFileInfo(LANDING_PATH + "/fresh.json", Instant.now().toEpochMilli());
    }

    private HdfsFileInfo staleFile() {
        return new HdfsFileInfo(LANDING_PATH + "/stale.json",
                Instant.now().minusSeconds(3600).toEpochMilli());
    }

    @Test
    @DisplayName("should pass when health is UP, listener enabled, and no backlog")
    void shouldPassWhenAllHealthy() throws IOException {
        enqueueHealth("{\"status\":\"UP\",\"components\":{\"mqListener\":{\"status\":\"UP\","
                + "\"details\":{\"listenerEnabled\":true,\"runningContainers\":1}}}}");
        when(hdfsFileOperations.listFiles(anyString())).thenReturn(List.of(freshFile()));

        assertThat(runner.runChecks()).isEqualTo(MonitorRunner.EXIT_OK);
    }

    @Test
    @DisplayName("should return 1 when the bridge is unreachable")
    void shouldFailWhenUnreachable() throws IOException {
        mockServer.shutdown();
        lenient().when(hdfsFileOperations.listFiles(anyString())).thenReturn(List.of());

        assertThat(runner.runChecks()).isEqualTo(MonitorRunner.EXIT_HEALTH_DOWN);
    }

    @Test
    @DisplayName("should return 1 when the mqListener component itself is DOWN")
    void shouldFailWhenListenerComponentDown() throws IOException {
        enqueueHealth("{\"status\":\"DOWN\",\"components\":{\"mqListener\":{\"status\":\"DOWN\","
                + "\"details\":{\"listenerEnabled\":true,\"runningContainers\":0}}}}");
        when(hdfsFileOperations.listFiles(anyString())).thenReturn(List.of());

        assertThat(runner.runChecks()).isEqualTo(MonitorRunner.EXIT_HEALTH_DOWN);
    }

    @Test
    @DisplayName("should PASS when only a dependency is DOWN and the listener is consuming")
    void shouldNotFailOnDependencyOutage() throws IOException {
        // The aggregate goes DOWN whenever the Kafka or HDFS indicator has a blip (both
        // run live probes per poll). The bridge is still consuming, so this must not be
        // reported as "bridge unreachable or health DOWN" — that was paging on-call for
        // a healthy bridge. Throughput loss is caught by MQ queue depth and the backlog check.
        enqueueHealth("{\"status\":\"DOWN\",\"components\":{"
                + "\"mqListener\":{\"status\":\"UP\",\"details\":{\"listenerEnabled\":true,\"runningContainers\":1}},"
                + "\"kafka\":{\"status\":\"DOWN\",\"details\":{\"error\":\"describeCluster timed out\"}},"
                + "\"hdfs\":{\"status\":\"UP\",\"details\":{\"accessible\":true}}}}");
        when(hdfsFileOperations.listFiles(anyString())).thenReturn(List.of(freshFile()));

        assertThat(runner.runChecks()).isEqualTo(MonitorRunner.EXIT_OK);
    }

    @Test
    @DisplayName("should return 2 when up but the listener is disabled")
    void shouldFailWhenNotConsuming() throws IOException {
        enqueueHealth("{\"status\":\"UP\",\"components\":{\"mqListener\":{\"status\":\"UP\","
                + "\"details\":{\"listenerEnabled\":false,\"warning\":\"NOT consuming\"}}}}");
        when(hdfsFileOperations.listFiles(anyString())).thenReturn(List.of());

        assertThat(runner.runChecks()).isEqualTo(MonitorRunner.EXIT_NOT_CONSUMING);
    }

    @Test
    @DisplayName("should return 3 when stale files exceed the backlog threshold")
    void shouldFailOnBacklog() throws IOException {
        enqueueHealth("{\"status\":\"UP\",\"components\":{\"mqListener\":{\"status\":\"UP\","
                + "\"details\":{\"listenerEnabled\":true}}}}");
        when(hdfsFileOperations.listFiles(anyString())).thenReturn(List.of(staleFile(), freshFile()));

        assertThat(runner.runChecks()).isEqualTo(MonitorRunner.EXIT_BACKLOG);
    }

    @Test
    @DisplayName("health failure outranks backlog failure")
    void healthFailureTakesPrecedence() throws IOException {
        enqueueHealth("{\"status\":\"DOWN\",\"components\":{\"mqListener\":{\"status\":\"DOWN\","
                + "\"details\":{\"listenerEnabled\":true,\"runningContainers\":0}}}}");
        when(hdfsFileOperations.listFiles(anyString())).thenReturn(List.of(staleFile()));

        assertThat(runner.runChecks()).isEqualTo(MonitorRunner.EXIT_HEALTH_DOWN);
    }

    @Test
    @DisplayName("should return 4 when the backlog check cannot be evaluated")
    void shouldReturnMonitorErrorWhenBacklogUnreadable() throws IOException {
        enqueueHealth("{\"status\":\"UP\",\"components\":{\"mqListener\":{\"status\":\"UP\","
                + "\"details\":{\"listenerEnabled\":true}}}}");
        when(hdfsFileOperations.listFiles(anyString())).thenThrow(new IOException("kerberos expired"));

        assertThat(runner.runChecks()).isEqualTo(MonitorRunner.EXIT_MONITOR_ERROR);
    }

    @Test
    @DisplayName("should exit 4 when the health payload hides the mqListener details")
    void shouldFailWhenListenerDetailsHidden() throws IOException {
        // A bare {"status":"UP"} (show-details hidden) makes the not-consuming check
        // unverifiable — that must be exit 4, never a silent PASS.
        enqueueHealth("{\"status\":\"UP\"}");
        when(hdfsFileOperations.listFiles(anyString())).thenReturn(List.of());

        assertThat(runner.runChecks()).isEqualTo(MonitorRunner.EXIT_MONITOR_ERROR);
    }

    @Test
    @DisplayName("should skip the backlog check when no landing path is configured")
    void shouldSkipBacklogWithoutLandingPath() {
        ReflectionTestUtils.setField(runner, "landingPath", "");
        enqueueHealth("{\"status\":\"UP\",\"components\":{\"mqListener\":"
                + "{\"status\":\"UP\",\"details\":{\"listenerEnabled\":true}}}}");

        assertThat(runner.runChecks()).isEqualTo(MonitorRunner.EXIT_OK);
    }

    @Test
    @DisplayName("should allow stale files up to backlog-max-files")
    void shouldRespectMaxFilesThreshold() throws IOException {
        ReflectionTestUtils.setField(runner, "backlogMaxFiles", 2);
        enqueueHealth("{\"status\":\"UP\",\"components\":{\"mqListener\":"
                + "{\"status\":\"UP\",\"details\":{\"listenerEnabled\":true}}}}");
        when(hdfsFileOperations.listFiles(anyString())).thenReturn(List.of(staleFile(), staleFile()));

        assertThat(runner.runChecks()).isEqualTo(MonitorRunner.EXIT_OK);
    }

    // Regression: the guard once used substring matching ("test-env".contains("test")),
    // which silently disabled monitor mode in the real test environment — monitor.sh's
    // default profile. Only the exact Surefire profile "test" may skip.
    @Test
    @DisplayName("test-environment guard must NOT trigger for the test-env deployment profile")
    void testEnvironmentGuardIsExactMatch() {
        when(environment.getActiveProfiles()).thenReturn(new String[]{"test-env"});
        assertThat((Boolean) ReflectionTestUtils.invokeMethod(runner, "isTestEnvironment"))
                .isFalse();

        when(environment.getActiveProfiles()).thenReturn(new String[]{"test"});
        assertThat((Boolean) ReflectionTestUtils.invokeMethod(runner, "isTestEnvironment"))
                .isTrue();
    }
}
