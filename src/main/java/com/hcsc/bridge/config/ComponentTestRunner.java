package com.hcsc.bridge.config;

import com.hcsc.bridge.api.EnrichmentException;
import com.hcsc.bridge.api.EnrichmentWrapperFactory;
import com.hcsc.bridge.api.MarketingPlanApiClient;
import com.hcsc.bridge.hdfs.HdfsFileOperations;
import com.hcsc.bridge.model.ParsedPayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Profile;
import org.springframework.core.annotation.Order;
import org.springframework.core.env.Environment;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Message;
import javax.jms.QueueBrowser;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Bring-up test harness for new-environment deployments. When {@code bridge.component-test}
 * names a mode, this runner exercises exactly ONE part of the bridge against real
 * infrastructure, logs a delimited PASS/FAIL report, and exits the JVM with an exit code:
 * {@code 0} = pass, {@code 1} = fail, {@code 2} = bad mode/args.
 *
 * <p>Mirrors {@link ValidateOnlyRunner}: gated by a property, guarded by
 * {@link #isTestEnvironment()} so Spring integration tests never kill the JVM, and exits via
 * {@link SpringApplication#exit}. Dependencies are injected lazily / via {@link ObjectProvider}
 * so a mode only initializes the beans it needs (e.g. the {@code api} mode never touches Kafka).
 *
 * <p>Ordered AFTER {@link ValidateOnlyRunner} (which has no explicit order and therefore runs at
 * {@code LOWEST_PRECEDENCE}); {@code bridge.validate-only} and {@code bridge.component-test} are
 * mutually exclusive in practice, and ordering only matters if both were set at once.
 */
@Component
@Profile("!local")
@Order
public class ComponentTestRunner implements ApplicationRunner {

    private static final Logger logger = LoggerFactory.getLogger(ComponentTestRunner.class);

    static final int EXIT_PASSED = 0;
    static final int EXIT_FAILED = 1;
    static final int EXIT_BAD_MODE = 2;

    private static final int MAX_BROWSE = 10;
    private static final int MAX_PREVIEW_CHARS = 300;
    // NOT a hardcoded value: the wait must exceed the producer's own send budget
    // (max.block.ms + delivery.timeout.ms), or a retriable condition (leader
    // unavailable, not enough in-sync replicas, unreachable leader) makes the test
    // report FAILED with a bare TimeoutException while the producer is still
    // retrying — the harness would then never show the real broker-side verdict.
    // Same coherence rule StartupConfigValidator enforces for the live publisher.
    @Value("${bridge.kafka.timeout-seconds:190}")
    private long kafkaTimeoutSeconds;

    @Value("${bridge.component-test:}")
    private String mode;

    @Value("${bridge.component-test-args:}")
    private String modeArgs;

    @Value("${bridge.mq.queue:BRIDGE.INPUT.QUEUE}")
    private String mqQueue;

    @Value("${bridge.kafka.topic:bridge-events}")
    private String kafkaTopic;

    @Value("${bridge.hdfs.base-path:/data/bridge/payloads}")
    private String hdfsBasePath;

    @Value("${bridge.hdfs.temp-suffix:.tmp}")
    private String hdfsTempSuffix;

    private final ApplicationContext applicationContext;
    private final Environment environment;
    private final ObjectProvider<ConnectionFactory> connectionFactoryProvider;
    private final ObjectProvider<MarketingPlanApiClient> apiClientProvider;
    private final ObjectProvider<EnrichmentWrapperFactory> wrapperFactoryProvider;
    private final ObjectProvider<KafkaTemplate<String, String>> kafkaTemplateProvider;
    private final ObjectProvider<HdfsFileOperations> hdfsFileOperationsProvider;

    @Autowired
    public ComponentTestRunner(
            ApplicationContext applicationContext,
            Environment environment,
            @Lazy ObjectProvider<ConnectionFactory> connectionFactoryProvider,
            @Lazy ObjectProvider<MarketingPlanApiClient> apiClientProvider,
            @Lazy ObjectProvider<EnrichmentWrapperFactory> wrapperFactoryProvider,
            @Lazy ObjectProvider<KafkaTemplate<String, String>> kafkaTemplateProvider,
            @Lazy ObjectProvider<HdfsFileOperations> hdfsFileOperationsProvider) {
        this.applicationContext = applicationContext;
        this.environment = environment;
        this.connectionFactoryProvider = connectionFactoryProvider;
        this.apiClientProvider = apiClientProvider;
        this.wrapperFactoryProvider = wrapperFactoryProvider;
        this.kafkaTemplateProvider = kafkaTemplateProvider;
        this.hdfsFileOperationsProvider = hdfsFileOperationsProvider;
    }

    private boolean isTestEnvironment() {
        // Guards Surefire tests (@ActiveProfiles("test")) from System.exit. EXACT
        // match only: a substring check would also match the real "test-env"
        // deployment profile and leave the JVM hanging after the component test.
        return Arrays.asList(environment.getActiveProfiles()).contains("test");
    }

    @Override
    public void run(ApplicationArguments args) {
        if (mode == null || mode.trim().isEmpty()) {
            logger.debug("Component-test mode is disabled, continuing normal startup");
            return;
        }

        logger.info("============================================");
        logger.info("RUNNING IN COMPONENT-TEST MODE: {}", mode);
        logger.info("============================================");

        final int exitCode = runComponentTest();

        logger.info("Exiting with code: {}", exitCode);
        if (isTestEnvironment()) {
            logger.warn("Component-test mode suppressed System.exit (exit code {}): the active 'test' profile is the Surefire exit-guard. Deployments use 'test-env', not 'test'.", exitCode);
            return;
        }
        SpringApplication.exit(applicationContext, () -> exitCode);
        System.exit(exitCode);
    }

    /**
     * Dispatches to the requested mode and returns the exit code. Package-private so tests can
     * capture the exit code without triggering {@link System#exit}.
     */
    int runComponentTest() {
        String normalized = mode.trim().toLowerCase();
        switch (normalized) {
            case "mq":
                return runMqTest();
            case "api":
                return runApiTest();
            case "kafka":
                return runKafkaTest();
            case "hdfs":
                return runHdfsTest();
            default:
                logger.error("============================================");
                logger.error("COMPONENT-TEST RESULT: BAD MODE");
                logger.error("Unknown mode '{}'. Expected one of: mq, api, kafka, hdfs", mode);
                logger.error("============================================");
                return EXIT_BAD_MODE;
        }
    }

    // ------------------------------------------------------------------------
    // Mode: mq — non-destructively browse the input queue
    // ------------------------------------------------------------------------
    private int runMqTest() {
        Connection connection = null;
        Session session = null;
        QueueBrowser browser = null;
        try {
            ConnectionFactory connectionFactory = connectionFactoryProvider.getObject();
            connection = connectionFactory.createConnection();
            connection.start();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            browser = session.createBrowser(session.createQueue(mqQueue));

            // Browsing is non-destructive: messages are enumerated, never consumed.
            Enumeration<?> messages = browser.getEnumeration();
            int seen = 0;
            String firstMessageId = null;
            String firstCorrelationId = null;
            int firstPayloadSize = -1;

            while (messages.hasMoreElements() && seen < MAX_BROWSE) {
                Message message = (Message) messages.nextElement();
                if (seen == 0) {
                    firstMessageId = message.getJMSMessageID();
                    firstCorrelationId = message.getJMSCorrelationID();
                    if (message instanceof TextMessage) {
                        String body = ((TextMessage) message).getText();
                        firstPayloadSize = body != null ? body.length() : 0;
                    }
                }
                seen++;
            }

            logger.info("============================================");
            logger.info("COMPONENT-TEST RESULT: PASSED (mq)");
            logger.info("  Queue: {}", mqQueue);
            logger.info("  Messages browsed (max {}): {}", MAX_BROWSE, seen);
            if (seen == 0) {
                logger.info("  Queue is empty");
            } else {
                // Never log the payload body — only metadata and its size.
                logger.info("  First JMSMessageID: {}", firstMessageId);
                logger.info("  First JMSCorrelationID: {}", firstCorrelationId);
                logger.info("  First payload size (chars): {}",
                        firstPayloadSize >= 0 ? firstPayloadSize : "n/a (not a TextMessage)");
            }
            logger.info("============================================");
            return EXIT_PASSED;

        } catch (Exception e) {
            logger.error("============================================");
            logger.error("COMPONENT-TEST RESULT: FAILED (mq)");
            logger.error("  Queue: {}", mqQueue);
            logger.error("  Error: {}", e.getMessage(), e);
            logger.error("============================================");
            return EXIT_FAILED;
        } finally {
            closeQuietly(browser);
            closeQuietly(session);
            closeQuietly(connection);
        }
    }

    // ------------------------------------------------------------------------
    // Mode: api — real enrichment call for <marketingPlanIdentifier>,<effectiveDate>
    // ------------------------------------------------------------------------
    private int runApiTest() {
        String[] parts = modeArgs != null ? modeArgs.split(",", -1) : new String[0];
        if (parts.length != 2 || parts[0].trim().isEmpty() || parts[1].trim().isEmpty()) {
            logger.error("============================================");
            logger.error("COMPONENT-TEST RESULT: BAD ARGS (api)");
            logger.error("Usage: --bridge.component-test-args=<marketingPlanIdentifier>,<effectiveDate>");
            logger.error("Example: --bridge.component-test-args=SPSH44PPOIMTO,2026-01-01");
            logger.error("============================================");
            return EXIT_BAD_MODE;
        }

        String planId = parts[0].trim();
        String effectiveDate = parts[1].trim();

        try {
            MarketingPlanApiClient apiClient = apiClientProvider.getObject();
            EnrichmentWrapperFactory wrapperFactory = wrapperFactoryProvider.getObject();

            ParsedPayload payload = new ParsedPayload(
                    "component-test",
                    planId,
                    "ComponentTest",
                    planId,
                    effectiveDate,
                    Collections.emptyMap(),
                    Instant.now(),
                    "{}");

            MarketingPlanApiClient.EnrichmentResult result = apiClient.enrich(payload);
            EnrichmentWrapperFactory.WrapperResult wrapper = wrapperFactory.build(result.getRawResponse(), null);

            String rawResponse = result.getRawResponse() != null ? result.getRawResponse().toString() : "";
            int rawSizeBytes = rawResponse.getBytes(StandardCharsets.UTF_8).length;
            String preview = rawResponse.length() > MAX_PREVIEW_CHARS
                    ? rawResponse.substring(0, MAX_PREVIEW_CHARS) + "..."
                    : rawResponse;

            logger.info("============================================");
            logger.info("COMPONENT-TEST RESULT: PASSED (api)");
            logger.info("  HTTP call: success");
            logger.info("  marketingPlanId: {}", result.getMarketingPlanId());
            logger.info("  changeEventTimeStamp: {}", wrapper.getChangeEventTimeStamp());
            logger.info("  changeEventTypeName: {}", wrapper.getChangeEventTypeName());
            logger.info("  Raw response size (bytes): {}", rawSizeBytes);
            logger.info("  Raw response preview (max {} chars): {}", MAX_PREVIEW_CHARS, preview);
            logger.info("============================================");
            return EXIT_PASSED;

        } catch (EnrichmentException e) {
            logger.error("============================================");
            logger.error("COMPONENT-TEST RESULT: FAILED (api)");
            logger.error("  marketingPlanId: {}", planId);
            logger.error("  HTTP status: {}", e.getStatusCode());
            logger.error("  Error: {}", e.getMessage());
            logger.error("============================================");
            return EXIT_FAILED;
        } catch (Exception e) {
            logger.error("============================================");
            logger.error("COMPONENT-TEST RESULT: FAILED (api)");
            logger.error("  marketingPlanId: {}", planId);
            logger.error("  Error: {}", e.getMessage(), e);
            logger.error("============================================");
            return EXIT_FAILED;
        }
    }

    // ------------------------------------------------------------------------
    // Mode: kafka — real publish (optional topic override)
    // ------------------------------------------------------------------------
    private int runKafkaTest() {
        // The topic must be passed EXPLICITLY: silently defaulting to the configured
        // (possibly production) topic would publish a contract-violating record that
        // downstream consumers classify as a legacy message and pass through as junk.
        // Publishing to the real topic is still possible — by naming it deliberately.
        String topic = (modeArgs != null && !modeArgs.trim().isEmpty()) ? modeArgs.trim() : null;
        if (topic == null) {
            logger.error("============================================");
            logger.error("COMPONENT-TEST RESULT: REFUSED (kafka)");
            logger.error("  No topic argument given. Refusing to publish a test record to the");
            logger.error("  configured topic '{}' implicitly.", kafkaTopic);
            logger.error("  Re-run with an explicit topic (a scratch topic is recommended):");
            logger.error("    --bridge.component-test=kafka --bridge.component-test-args=<topic>");
            logger.error("============================================");
            return EXIT_BAD_MODE;
        }
        String key = "component-test-" + UUID.randomUUID();
        String value = "{\"componentTest\":true,\"source\":\"mq-kafka-bridge-component-test\","
                + "\"timestamp\":\"" + Instant.now() + "\"}";

        logger.warn("Component-test 'kafka' publishes a REAL (clearly marked) message to topic '{}'.", topic);

        try {
            KafkaTemplate<String, String> kafkaTemplate = kafkaTemplateProvider.getObject();
            ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send(topic, key, value);
            SendResult<String, String> result = future.get(kafkaTimeoutSeconds, TimeUnit.SECONDS);

            logger.info("============================================");
            logger.info("COMPONENT-TEST RESULT: PASSED (kafka)");
            logger.info("  Topic: {}", result.getRecordMetadata().topic());
            logger.info("  Partition: {}", result.getRecordMetadata().partition());
            logger.info("  Offset: {}", result.getRecordMetadata().offset());
            logger.info("  Key: {}", key);
            logger.info("============================================");
            return EXIT_PASSED;

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("============================================");
            logger.error("COMPONENT-TEST RESULT: FAILED (kafka)");
            logger.error("  Topic: {}", topic);
            logger.error("  Error: interrupted while publishing - {}", e.getMessage());
            logger.error("============================================");
            return EXIT_FAILED;
        } catch (java.util.concurrent.TimeoutException e) {
            // TimeoutException carries no message ("Error: null" is useless at 2am).
            // Reaching this means the producer connected, authenticated and got
            // metadata — the record simply was not acknowledged in time, which points
            // at the LEADER for this partition, not at connectivity in general.
            logger.error("============================================");
            logger.error("COMPONENT-TEST RESULT: FAILED (kafka)");
            logger.error("  Topic: {}", topic);
            logger.error("  Error: no broker acknowledgement within {}s (TimeoutException)",
                    kafkaTimeoutSeconds);
            logger.error("  The producer DID connect, authenticate and fetch metadata, so this is");
            logger.error("  not a login/TLS problem. Likeliest causes, in order:");
            logger.error("    1. the partition LEADER is on a broker this host cannot reach");
            logger.error("       (check every bootstrap broker with: nc -zv <host> 9093)");
            logger.error("    2. acks=all cannot be satisfied — min.insync.replicas > available ISR");
            logger.error("    3. producer throttling/quota on this principal");
            logger.error("  Re-run with --logging.level.org.apache.kafka.clients.producer.internals.Sender=DEBUG");
            logger.error("  to see the per-retry broker error (NOT_ENOUGH_REPLICAS, LEADER_NOT_AVAILABLE, ...)");
            logger.error("============================================");
            return EXIT_FAILED;
        } catch (Exception e) {
            logger.error("============================================");
            logger.error("COMPONENT-TEST RESULT: FAILED (kafka)");
            logger.error("  Topic: {}", topic);
            logger.error("  Error: {}: {}", e.getClass().getSimpleName(), e.getMessage(), e);
            logger.error("============================================");
            return EXIT_FAILED;
        }
    }

    // ------------------------------------------------------------------------
    // Mode: hdfs — write / verify-checksum / cleanup roundtrip
    // ------------------------------------------------------------------------
    private int runHdfsTest() {
        // Tolerate a trailing slash on the configured base path
        String base = hdfsBasePath.replaceAll("/+$", "");
        String targetPath = base + "/component-test/" + UUID.randomUUID() + ".json";
        String tempPath = targetPath + hdfsTempSuffix;
        String content = "{\"componentTest\":true,\"timestamp\":\"" + Instant.now() + "\"}";
        byte[] contentBytes = content.getBytes(StandardCharsets.UTF_8);

        HdfsFileOperations hdfs;
        try {
            hdfs = hdfsFileOperationsProvider.getObject();
        } catch (Exception e) {
            logger.error("============================================");
            logger.error("COMPONENT-TEST RESULT: FAILED (hdfs)");
            logger.error("  Error obtaining HDFS operations: {}", e.getMessage(), e);
            logger.error("============================================");
            return EXIT_FAILED;
        }

        boolean written = false;
        try {
            // Same safe sequence as production: mkdirs parent, write to .tmp, rename, verify checksum.
            String parentPath = targetPath.substring(0, targetPath.lastIndexOf('/'));
            hdfs.mkdirs(parentPath);

            try (OutputStream out = hdfs.create(tempPath)) {
                out.write(contentBytes);
                out.flush();
            }

            boolean renamed = hdfs.rename(tempPath, targetPath);
            if (!renamed) {
                throw new IllegalStateException("Failed to rename temp file to target: " + targetPath);
            }
            written = true;

            String remoteChecksum = hdfs.getFileChecksum(targetPath);
            String localChecksum = sha256Hex(contentBytes);

            if (!localChecksum.equals(remoteChecksum)) {
                logger.error("============================================");
                logger.error("COMPONENT-TEST RESULT: FAILED (hdfs)");
                logger.error("  Path: {}", targetPath);
                logger.error("  Checksum mismatch: expected {} but got {}", localChecksum, remoteChecksum);
                logger.error("============================================");
                return EXIT_FAILED;
            }

            logger.info("============================================");
            logger.info("COMPONENT-TEST RESULT: PASSED (hdfs)");
            logger.info("  Path: {}", targetPath);
            logger.info("  Bytes written: {}", contentBytes.length);
            logger.info("  Checksum verified: {}", localChecksum);
            logger.info("============================================");
            return EXIT_PASSED;

        } catch (Exception e) {
            logger.error("============================================");
            logger.error("COMPONENT-TEST RESULT: FAILED (hdfs)");
            logger.error("  Path: {}", targetPath);
            logger.error("  Error: {}", e.getMessage(), e);
            logger.error("============================================");
            return EXIT_FAILED;
        } finally {
            cleanupHdfs(hdfs, written ? targetPath : tempPath);
        }
    }

    private void cleanupHdfs(HdfsFileOperations hdfs, String path) {
        try {
            hdfs.delete(path);
            logger.info("Component-test file cleaned up: {}", path);
        } catch (Exception e) {
            logger.warn("Failed to delete component-test file; leftover path: {}", path, e);
        }
    }

    private String sha256Hex(byte[] content) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(content);
            StringBuilder sb = new StringBuilder();
            for (byte b : hash) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (Exception e) {
            throw new IllegalStateException("SHA-256 algorithm not available", e);
        }
    }

    private void closeQuietly(AutoCloseable closeable) {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        } catch (Exception e) {
            logger.warn("Error closing {}: {}", closeable.getClass().getSimpleName(), e.getMessage());
        }
    }
}
