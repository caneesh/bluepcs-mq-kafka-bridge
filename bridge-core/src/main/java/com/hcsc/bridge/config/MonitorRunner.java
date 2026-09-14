package com.hcsc.bridge.config;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hcsc.bridge.hdfs.HdfsFileOperations;
import com.hcsc.bridge.hdfs.HdfsFileOperations.HdfsFileInfo;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Read-only monitoring mode for external schedulers (Control-M). Activated with
 * {@code --bridge.monitor.enabled=true}; checks the RUNNING bridge instance's health
 * endpoint and the HDFS landing-directory backlog, prints a MONITOR RESULT block for
 * the scheduler's sysout capture, and exits with a code the scheduler can route on.
 *
 * The monitor never remediates — restarts belong to systemd/the watchdog; this mode
 * exists to surface failures into enterprise alerting. Run it via scripts/monitor.sh,
 * which disables the web server and the MQ listener for the monitor JVM.
 */
@Component
public class MonitorRunner implements ApplicationRunner {

    private static final Logger logger = LoggerFactory.getLogger(MonitorRunner.class);

    static final int EXIT_OK = 0;
    static final int EXIT_HEALTH_DOWN = 1;
    static final int EXIT_NOT_CONSUMING = 2;
    static final int EXIT_BACKLOG = 3;
    static final int EXIT_MONITOR_ERROR = 4;

    @Value("${bridge.monitor.enabled:false}")
    private boolean monitorEnabled;

    @Value("${bridge.monitor.health-url:http://localhost:8080/actuator/health}")
    private String healthUrl;

    @Value("${bridge.monitor.backlog-age-minutes:30}")
    private long backlogAgeMinutes;

    @Value("${bridge.monitor.backlog-max-files:0}")
    private int backlogMaxFiles;

    @Value("${bridge.hdfs.base-path:}")
    private String landingPath;

    private final HdfsFileOperations hdfsFileOperations;
    private final ApplicationContext applicationContext;
    private final Environment environment;
    private final OkHttpClient httpClient;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Autowired
    public MonitorRunner(HdfsFileOperations hdfsFileOperations,
                         ApplicationContext applicationContext,
                         Environment environment) {
        this(hdfsFileOperations, applicationContext, environment,
                new OkHttpClient.Builder()
                        .connectTimeout(10, TimeUnit.SECONDS)
                        // The aggregate endpoint runs LIVE probes per poll: the Kafka
                        // indicator alone waits 2x5s on describeCluster, plus an HDFS
                        // namenode RPC. A 10s read timeout made a slow-but-healthy
                        // response look like "bridge unreachable".
                        .readTimeout(30, TimeUnit.SECONDS)
                        .build());
    }

    MonitorRunner(HdfsFileOperations hdfsFileOperations,
                  ApplicationContext applicationContext,
                  Environment environment,
                  OkHttpClient httpClient) {
        this.hdfsFileOperations = hdfsFileOperations;
        this.applicationContext = applicationContext;
        this.environment = environment;
        this.httpClient = httpClient;
    }

    private boolean isTestEnvironment() {
        // Guards Surefire tests (@ActiveProfiles("test")) from System.exit. EXACT
        // match only: a substring check ("test-env".contains("test")) would also
        // match the real test deployment profile and silently skip every check
        // while monitor.sh reports PASSED.
        return Arrays.asList(environment.getActiveProfiles()).contains("test");
    }

    @Override
    public void run(ApplicationArguments args) {
        if (!monitorEnabled) {
            logger.debug("Monitor mode is disabled, continuing normal startup");
            return;
        }

        if (isTestEnvironment()) {
            logger.warn("Monitor mode requested but suppressed: the active 'test' profile is the Surefire exit-guard. Deployments use 'test-env', not 'test'.");
            return;
        }

        logger.info("============================================");
        logger.info("RUNNING IN MONITOR MODE");
        logger.info("============================================");

        final int exitCode = runChecks();

        logger.info("Exiting with code: {}", exitCode);
        DiagnosticJvmExit.exit(applicationContext, exitCode);
    }

    /**
     * Runs both checks and returns the exit code (severity order: health > consuming >
     * backlog). Both checks always execute so the summary shows the complete picture.
     */
    int runChecks() {
        int healthCode;
        int backlogCode;
        // Full stack on exit-4 paths: for an NPE, getMessage() alone prints ": null" and
        // the engineer routing the alert has nothing to go on.
        try {
            healthCode = checkHealth();
        } catch (Exception e) {
            logger.error("MONITOR: health check could not be evaluated", e);
            healthCode = EXIT_MONITOR_ERROR;
        }
        try {
            backlogCode = checkBacklog();
        } catch (Exception e) {
            logger.error("MONITOR: backlog check could not be evaluated", e);
            backlogCode = EXIT_MONITOR_ERROR;
        }

        int exitCode;
        if (healthCode == EXIT_HEALTH_DOWN || healthCode == EXIT_NOT_CONSUMING) {
            exitCode = healthCode;
        } else if (backlogCode == EXIT_BACKLOG) {
            exitCode = backlogCode;
        } else if (healthCode != EXIT_OK) {
            exitCode = healthCode;
        } else {
            exitCode = backlogCode;
        }

        if (exitCode == EXIT_OK) {
            logger.info("============================================");
            logger.info("MONITOR RESULT: PASSED");
            logger.info("============================================");
        } else {
            logger.error("============================================");
            logger.error("MONITOR RESULT: FAILED (exit code {})", exitCode);
            logger.error("============================================");
        }
        return exitCode;
    }

    /**
     * Checks the running bridge's health endpoint. DOWN/unreachable is a hard failure;
     * an UP response with the mqListener component reporting listenerEnabled=false is
     * flagged separately — the bridge is alive but deliberately not consuming, which in
     * a 24/7 deployment means someone forgot to enable the listener.
     */
    private int checkHealth() throws Exception {
        Request request = new Request.Builder().url(healthUrl).get().build();

        JsonNode health;
        try (Response response = httpClient.newCall(request).execute()) {
            String body = response.body() != null ? response.body().string() : "";
            if (body.isEmpty()) {
                logger.error("MONITOR: health endpoint returned an empty body (HTTP {})", response.code());
                return EXIT_HEALTH_DOWN;
            }
            health = objectMapper.readTree(body);
        } catch (java.io.IOException e) {
            logger.error("MONITOR: bridge unreachable at {}: {}", healthUrl, e.getMessage());
            return EXIT_HEALTH_DOWN;
        }

        // Never report PASSED on an unverifiable check: with
        // management.endpoint.health.show-details hidden (e.g. when_authorized with no
        // Spring Security on the classpath) the response is a bare {"status":"UP"} and
        // the listener state is unobservable — silently skipping the check here is how
        // "UP but consuming nothing" goes unnoticed.
        JsonNode listener = health.path("components").path("mqListener");
        JsonNode listenerDetails = listener.path("details");
        if (listenerDetails.isMissingNode() || !listenerDetails.has("listenerEnabled")) {
            logger.error("MONITOR: health response carries no mqListener details — cannot verify "
                    + "the listener is consuming. Check management.endpoint.health.show-details "
                    + "(must be 'always' for this deployment) and that the mqListener health "
                    + "indicator is active");
            return EXIT_MONITOR_ERROR;
        }

        // The BRIDGE's liveness is the mqListener component, NOT the aggregate status.
        // The aggregate also folds in the Kafka and HDFS indicators, which run live
        // probes on EVERY poll (AdminClient.describeCluster, a namenode RPC) and flip
        // DOWN on any dependency blip — treating that as "bridge unreachable or health
        // DOWN" (exit 1 = page) fires false alarms while the bridge is processing fine.
        String listenerStatus = listener.path("status").asText("");
        if (!"UP".equals(listenerStatus)) {
            logger.error("MONITOR: mqListener component is {} — the bridge is not processing. Details: {}",
                    listenerStatus.isEmpty() ? "unknown" : listenerStatus, listenerDetails);
            return EXIT_HEALTH_DOWN;
        }
        if (!listenerDetails.path("listenerEnabled").asBoolean(true)) {
            logger.error("MONITOR: bridge is UP but the MQ listener is disabled — NOT consuming messages");
            return EXIT_NOT_CONSUMING;
        }

        // Dependency trouble is reported, never failed on: loss of throughput is caught
        // definitively by MQ queue depth and by the backlog check, and the supervisors
        // deliberately ignore it too (they poll the liveness group).
        List<String> degraded = degradedDependencies(health);
        if (!degraded.isEmpty()) {
            logger.warn("MONITOR: DEPENDENCY DEGRADED (the bridge itself is healthy and consuming): {}. "
                    + "Not failing the job — watch MQ queue depth for actual throughput loss.", degraded);
        }

        logger.info("MONITOR: health check passed (mqListener UP and enabled; aggregate status {})",
                health.path("status").asText("unknown"));
        return EXIT_OK;
    }

    /** Component names (excluding mqListener) whose status is not UP. */
    private List<String> degradedDependencies(JsonNode health) {
        List<String> degraded = new ArrayList<>();
        Iterator<Map.Entry<String, JsonNode>> components = health.path("components").fields();
        while (components.hasNext()) {
            Map.Entry<String, JsonNode> component = components.next();
            if ("mqListener".equals(component.getKey())) {
                continue;
            }
            String status = component.getValue().path("status").asText("");
            if (!status.isEmpty() && !"UP".equals(status)) {
                degraded.add(component.getKey() + "=" + status);
            }
        }
        return degraded;
    }

    /**
     * Counts landing-directory files older than the threshold. Files should be picked
     * up by the downstream consumer promptly; stale files mean the consumer stalled
     * even though the bridge itself is healthy.
     */
    private int checkBacklog() throws Exception {
        if (landingPath == null || landingPath.isEmpty()) {
            logger.info("MONITOR: backlog check skipped (bridge.hdfs.base-path not configured)");
            return EXIT_OK;
        }

        long cutoffMillis = Instant.now().minusSeconds(backlogAgeMinutes * 60).toEpochMilli();
        List<HdfsFileInfo> stale = hdfsFileOperations.listFiles(landingPath).stream()
                .filter(f -> f.getModificationTimeMillis() < cutoffMillis)
                .collect(Collectors.toList());

        if (stale.size() > backlogMaxFiles) {
            HdfsFileInfo oldest = stale.stream()
                    .min(Comparator.comparingLong(HdfsFileInfo::getModificationTimeMillis))
                    .orElse(null);
            logger.error("MONITOR: {} file(s) older than {} min in landing dir {} (max allowed {}). Oldest: {}",
                    stale.size(), backlogAgeMinutes, landingPath, backlogMaxFiles, oldest);
            return EXIT_BACKLOG;
        }

        logger.info("MONITOR: backlog check passed ({} stale file(s), max allowed {})",
                stale.size(), backlogMaxFiles);
        return EXIT_OK;
    }
}
