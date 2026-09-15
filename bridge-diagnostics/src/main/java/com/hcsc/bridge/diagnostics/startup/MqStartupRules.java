package com.hcsc.bridge.diagnostics.startup;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;

/**
 * The MQ configuration rules both bridges enforce at startup, defined once.
 *
 * <p>These used to be re-implemented per application, and the copies had drifted: the
 * second bridge checked fewer things than the first, so the same misconfiguration failed
 * fast in one process and started happily in the other. Each application still owns its own
 * property values - they are different deployments - but the rules applied to them are
 * these.
 */
public final class MqStartupRules {

    private static final Logger logger = LoggerFactory.getLogger(MqStartupRules.class);

    private final String mqHost;
    private final int mqPort;
    private final String mqQueueManager;
    private final String mqChannel;
    private final String mqQueue;
    private final String mqUsername;
    private final String mqPassword;
    private final boolean mqSslEnabled;
    private final String mqSslCipherSuite;
    private final boolean mqListenerEnabled;
    private final boolean requireListenerEnabled;
    private final boolean diagnosticMode;
    /**
     * Appended to the "queue is required" error. The rule is shared, but the environment
     * variable that supplies the queue is per-application, and naming the right one is the
     * difference between an actionable startup failure and a puzzle.
     */
    private final String queueHint;

    public MqStartupRules(String mqHost, int mqPort, String mqQueueManager, String mqChannel, String mqQueue, String mqUsername, String mqPassword, boolean mqSslEnabled, String mqSslCipherSuite, boolean mqListenerEnabled, boolean requireListenerEnabled, boolean diagnosticMode,
                          String queueHint) {
        this.mqHost = mqHost;
        this.mqPort = mqPort;
        this.mqQueueManager = mqQueueManager;
        this.mqChannel = mqChannel;
        this.mqQueue = mqQueue;
        this.mqUsername = mqUsername;
        this.mqPassword = mqPassword;
        this.mqSslEnabled = mqSslEnabled;
        this.mqSslCipherSuite = mqSslCipherSuite;
        this.mqListenerEnabled = mqListenerEnabled;
        this.requireListenerEnabled = requireListenerEnabled;
        this.diagnosticMode = diagnosticMode;
        this.queueHint = queueHint == null ? "" : queueHint;
    }

    public void validateMqConfig(List<String> errors, List<String> warnings) {
        logger.info("Validating MQ configuration...");

        if (isBlank(mqHost)) {
            errors.add("[MQ] bridge.mq.host is required");
        } else {
            logger.info("[MQ] Host: {}", mqHost);
        }

        if (mqPort <= 0 || mqPort > 65535) {
            errors.add("[MQ] bridge.mq.port must be between 1 and 65535");
        } else {
            logger.info("[MQ] Port: {}", mqPort);
        }

        if (isBlank(mqQueueManager)) {
            errors.add("[MQ] bridge.mq.queue-manager is required");
        } else {
            logger.info("[MQ] Queue Manager: {}", mqQueueManager);
        }

        if (isBlank(mqChannel)) {
            errors.add("[MQ] bridge.mq.channel is required");
        } else {
            logger.info("[MQ] Channel: {}", mqChannel);
        }

        if (isBlank(mqQueue)) {
            errors.add("[MQ] bridge.mq.queue is required" + queueHint);
        } else {
            logger.info("[MQ] Queue: {}", mqQueue);
        }

        if (isBlank(mqUsername)) {
            warnings.add("[MQ] bridge.mq.username not set - anonymous connection");
        } else {
            logger.info("[MQ] Username: {}", mqUsername);
        }

        if (isBlank(mqPassword) && !isBlank(mqUsername)) {
            // Some queue managers authenticate by user id / channel auth only
            warnings.add("[MQ] bridge.mq.username set without a password - connecting without MQCSP password authentication");
        } else if (!isBlank(mqPassword)) {
            logger.info("[MQ] Password: ********");
        }

        // SSL coherence: enabled without a cipher fails at connection-factory creation
        // with an opaque error; a cipher without the flag works (legacy trigger) but is
        // implicit — surface both at validation time.
        if (mqSslEnabled && isBlank(mqSslCipherSuite)) {
            errors.add("[MQ] bridge.mq.ssl.enabled=true requires bridge.mq.ssl.cipher-suite "
                    + "(must match the SVRCONN channel's SSLCIPH)");
        } else if (!mqSslEnabled && !isBlank(mqSslCipherSuite)) {
            warnings.add("[MQ] bridge.mq.ssl.cipher-suite is set without bridge.mq.ssl.enabled=true "
                    + "- SSL is still configured (legacy trigger); set enabled=true to be explicit");
        }
    }
    public void validateListenerGate(List<String> errors, List<String> warnings) {
        if (diagnosticMode) {
            return;
        }
        if (requireListenerEnabled && !mqListenerEnabled) {
            errors.add("[MQ] bridge.mq.require-listener-enabled=true but the MQ listener is disabled - "
                    + "the app would run healthy while consuming nothing. Pass "
                    + "--bridge.mq.listener-enabled=true for go-live, or "
                    + "--bridge.mq.require-listener-enabled=false for a deliberate safe-start.");
        } else if (!mqListenerEnabled) {
            warnings.add("[MQ] listener disabled (safe-start): the app will report UP but consume nothing");
        }
    }
    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }
}
