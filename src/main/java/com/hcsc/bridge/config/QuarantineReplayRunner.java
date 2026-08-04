package com.hcsc.bridge.config;

import com.hcsc.bridge.hdfs.HdfsFileOperations;
import com.hcsc.bridge.hdfs.HdfsFileOperations.HdfsFileInfo;
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
import org.springframework.stereotype.Component;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.util.Arrays;
import java.util.List;

/**
 * Operational replay tool for quarantined payloads. Quarantine is otherwise a one-way
 * door: a message that was quarantined + acked (parse failure, poison discard) exists
 * only as {@code <error-path>/<eventId>.json} with no automated way back onto the queue.
 * This runner re-puts a quarantined payload to the input queue as a brand-new message.
 *
 * <p>Activated with {@code --bridge.replay=list} (show what is quarantined) or
 * {@code --bridge.replay=<hdfsPath>[,<hdfsPath>...]} (replay those files). Exit codes:
 * {@code 0} = all replayed (or list printed), {@code 1} = at least one file failed,
 * {@code 2} = bad arguments. Run via scripts/quarantine-replay.sh.
 *
 * <p>Semantics of a replay:
 * <ul>
 *   <li>The payload is sent verbatim as a new TextMessage — it gets a fresh JMSMessageID
 *       and therefore a fresh eventId; the bridge processes it as a brand-new event.
 *       Quarantined messages never reached Kafka, so replay cannot create downstream
 *       duplicates by itself.</li>
 *   <li>After a successful put, the quarantine file is moved to
 *       {@code <error-path>/replayed/} so a re-run cannot double-replay it. A failed
 *       move is reported as a failure (exit 1) precisely because of that re-run risk.</li>
 *   <li>Replay while the bridge is running is the normal case: the running listener
 *       picks the message up. The replay JVM itself never consumes (listener disabled
 *       by the wrapper script).</li>
 * </ul>
 *
 * <p>Mirrors {@link ComponentTestRunner}: property-gated, exits via
 * {@link SpringApplication#exit}, guarded by {@link #isTestEnvironment()} so Spring
 * integration tests never kill the JVM.
 */
@Component
@Profile("!local")
@Order
public class QuarantineReplayRunner implements ApplicationRunner {

    private static final Logger logger = LoggerFactory.getLogger(QuarantineReplayRunner.class);

    static final int EXIT_OK = 0;
    static final int EXIT_FAILED = 1;
    static final int EXIT_BAD_ARGS = 2;

    private static final String REPLAYED_SUBDIR = "replayed";

    @Value("${bridge.replay:}")
    private String replayArg;

    @Value("${bridge.mq.queue:BRIDGE.INPUT.QUEUE}")
    private String mqQueue;

    @Value("${bridge.hdfs.base-path:/data/bridge/payloads}")
    private String basePath;

    @Value("${bridge.hdfs.error-path:}")
    private String errorPath;

    private final ApplicationContext applicationContext;
    private final Environment environment;
    private final ObjectProvider<ConnectionFactory> connectionFactoryProvider;
    private final ObjectProvider<HdfsFileOperations> hdfsFileOperationsProvider;

    @Autowired
    public QuarantineReplayRunner(
            ApplicationContext applicationContext,
            Environment environment,
            @Lazy ObjectProvider<ConnectionFactory> connectionFactoryProvider,
            @Lazy ObjectProvider<HdfsFileOperations> hdfsFileOperationsProvider) {
        this.applicationContext = applicationContext;
        this.environment = environment;
        this.connectionFactoryProvider = connectionFactoryProvider;
        this.hdfsFileOperationsProvider = hdfsFileOperationsProvider;
    }

    private boolean isTestEnvironment() {
        // EXACT match only — same reasoning as ComponentTestRunner: "test-env" is a real
        // deployment profile and must not suppress the exit.
        return Arrays.asList(environment.getActiveProfiles()).contains("test");
    }

    private String effectiveErrorPath() {
        String cleanBase = basePath.replaceAll("/+$", "");
        return (errorPath == null || errorPath.trim().isEmpty())
                ? cleanBase + "/errors"
                : errorPath.replaceAll("/+$", "");
    }

    @Override
    public void run(ApplicationArguments args) {
        if (replayArg == null || replayArg.trim().isEmpty()) {
            logger.debug("Replay mode is disabled, continuing normal startup");
            return;
        }

        logger.info("============================================");
        logger.info("RUNNING IN QUARANTINE-REPLAY MODE");
        logger.info("============================================");

        final int exitCode = runReplay();

        logger.info("Exiting with code: {}", exitCode);
        if (isTestEnvironment()) {
            logger.warn("Replay mode suppressed System.exit (exit code {}): the active 'test' profile is the Surefire exit-guard. Deployments use 'test-env', not 'test'.", exitCode);
            return;
        }
        SpringApplication.exit(applicationContext, () -> exitCode);
        System.exit(exitCode);
    }

    /** Package-private so tests can capture the exit code without System.exit. */
    int runReplay() {
        String normalized = replayArg.trim();
        if ("list".equalsIgnoreCase(normalized)) {
            return listQuarantine();
        }
        return replayFiles(normalized.split(","));
    }

    private int listQuarantine() {
        String dir = effectiveErrorPath();
        try {
            HdfsFileOperations hdfs = hdfsFileOperationsProvider.getObject();
            List<HdfsFileInfo> files = hdfs.listFiles(dir);
            logger.info("Quarantine directory: {} — {} file(s)", dir, files.size());
            for (HdfsFileInfo file : files) {
                logger.info("  {}  (modified {})",
                        file.getPath(), java.time.Instant.ofEpochMilli(file.getModificationTimeMillis()));
            }
            if (files.isEmpty()) {
                logger.info("  (empty — nothing to replay)");
            }
            logger.info("REPLAY RESULT: LIST OK");
            return EXIT_OK;
        } catch (Exception e) {
            logger.error("REPLAY RESULT: FAILED — could not list {}", dir, e);
            return EXIT_FAILED;
        }
    }

    private int replayFiles(String[] paths) {
        int replayed = 0;
        int failed = 0;

        Connection connection = null;
        Session session = null;
        MessageProducer producer = null;
        try {
            HdfsFileOperations hdfs = hdfsFileOperationsProvider.getObject();
            ConnectionFactory connectionFactory = connectionFactoryProvider.getObject();
            connection = connectionFactory.createConnection();
            connection.start();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            producer = session.createProducer(session.createQueue(mqQueue));

            for (String rawPath : paths) {
                String path = rawPath.trim();
                if (path.isEmpty()) {
                    continue;
                }
                if (replayOne(hdfs, session, producer, path)) {
                    replayed++;
                } else {
                    failed++;
                }
            }
        } catch (Exception e) {
            logger.error("REPLAY RESULT: FAILED — could not set up MQ/HDFS for replay", e);
            return EXIT_FAILED;
        } finally {
            closeQuietly(producer, session, connection);
        }

        if (replayed == 0 && failed == 0) {
            logger.error("REPLAY RESULT: BAD ARGS — no file paths given. "
                    + "Usage: --bridge.replay=list | --bridge.replay=<hdfsPath>[,<hdfsPath>...]");
            return EXIT_BAD_ARGS;
        }
        logger.info("REPLAY RESULT: {} — {} replayed, {} failed",
                failed == 0 ? "OK" : "PARTIAL FAILURE", replayed, failed);
        return failed == 0 ? EXIT_OK : EXIT_FAILED;
    }

    /**
     * Replays a single quarantine file: read → put to the input queue → move the file
     * to the replayed/ subdirectory. Never logs the payload itself (may contain PHI).
     */
    private boolean replayOne(HdfsFileOperations hdfs, Session session, MessageProducer producer,
                              String path) {
        try {
            if (!hdfs.exists(path)) {
                logger.error("Replay failed: {} does not exist (already replayed? check {}/{})",
                        path, effectiveErrorPath(), REPLAYED_SUBDIR);
                return false;
            }
            String payload = hdfs.readUtf8(path);

            TextMessage message = session.createTextMessage(payload);
            producer.send(message);
            logger.info("Replayed {} to {} as new JMSMessageID {} ({} chars)",
                    path, mqQueue, message.getJMSMessageID(), payload.length());

            // Move out of the quarantine dir so a re-run cannot double-replay. A failure
            // here counts as a failure of the whole replay for exactly that reason.
            String replayedDir = effectiveErrorPath() + "/" + REPLAYED_SUBDIR;
            String fileName = path.substring(path.lastIndexOf('/') + 1);
            hdfs.mkdirs(replayedDir);
            String movedTo = replayedDir + "/" + fileName;
            if (!hdfs.rename(path, movedTo)) {
                logger.error("Replayed {} but could NOT move it to {} — move it manually "
                        + "NOW or a re-run will replay it again", path, movedTo);
                return false;
            }
            logger.info("Moved {} -> {}", path, movedTo);
            return true;
        } catch (Exception e) {
            logger.error("Replay failed for {}", path, e);
            return false;
        }
    }

    private void closeQuietly(MessageProducer producer, Session session, Connection connection) {
        try {
            if (producer != null) {
                producer.close();
            }
        } catch (Exception e) {
            logger.debug("Failed to close producer", e);
        }
        try {
            if (session != null) {
                session.close();
            }
        } catch (Exception e) {
            logger.debug("Failed to close session", e);
        }
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (Exception e) {
            logger.debug("Failed to close connection", e);
        }
    }
}
