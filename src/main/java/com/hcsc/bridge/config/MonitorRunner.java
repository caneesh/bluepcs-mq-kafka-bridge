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
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
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
                        .readTimeout(10, TimeUnit.SECONDS)
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
        String[] activeProfiles = environment.getActiveProfiles();
        return Arrays.asList(activeProfiles).contains("test") ||
               System.getProperty("spring.test.context") != null ||
               applicationContext.getEnvironment().getProperty("spring.profiles.active", "").contains("test");
    }

    @Override
    public void run(ApplicationArguments args) {
        if (!monitorEnabled) {
            logger.debug("Monitor mode is disabled, continuing normal startup");
            return;
        }

        if (isTestEnvironment()) {
            logger.debug("Monitor mode skipped in test environment");
            return;
        }

        logger.info("============================================");
        logger.info("RUNNING IN MONITOR MODE");
        logger.info("============================================");

        final int exitCode = runChecks();

        logger.info("Exiting with code: {}", exitCode);
        SpringApplication.exit(applicationContext, () -> exitCode);
        System.exit(exitCode);
    }

    /**
     * Runs both checks and returns the exit code (severity order: health > consuming >
     * backlog). Both checks always execute so the summary shows the complete picture.
     */
    int runChecks() {
        int healthCode;
        int backlogCode;
        try {
            healthCode = checkHealth();
        } catch (Exception e) {
            logger.error("MONITOR: health check could not be evaluated: {}", e.getMessage());
            healthCode = EXIT_MONITOR_ERROR;
        }
        try {
            backlogCode = checkBacklog();
        } catch (Exception e) {
            logger.error("MONITOR: backlog check could not be evaluated: {}", e.getMessage());
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

        String status = health.path("status").asText("");
        if (!"UP".equals(status)) {
            logger.error("MONITOR: bridge health is {} (expected UP)", status.isEmpty() ? "unknown" : status);
            return EXIT_HEALTH_DOWN;
        }

        JsonNode listenerDetails = health.path("components").path("mqListener").path("details");
        if (!listenerDetails.isMissingNode()
                && listenerDetails.has("listenerEnabled")
                && !listenerDetails.path("listenerEnabled").asBoolean(true)) {
            logger.error("MONITOR: bridge is UP but the MQ listener is disabled — NOT consuming messages");
            return EXIT_NOT_CONSUMING;
        }

        logger.info("MONITOR: health check passed (status UP)");
        return EXIT_OK;
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
