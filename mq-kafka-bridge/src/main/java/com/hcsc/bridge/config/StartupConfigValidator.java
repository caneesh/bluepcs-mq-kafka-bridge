package com.hcsc.bridge.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import com.hcsc.bridge.diagnostics.startup.HdfsStartupRules;
import com.hcsc.bridge.diagnostics.startup.KafkaStartupRules;
import com.hcsc.bridge.diagnostics.startup.MqStartupRules;
import com.hcsc.bridge.diagnostics.startup.StsStartupRules;

import javax.annotation.PostConstruct;
import java.io.File;
import java.util.ArrayList;
import java.util.List;

@Component
@Profile("!local")
public class StartupConfigValidator {

    private static final Logger logger = LoggerFactory.getLogger(StartupConfigValidator.class);

    @Value("${bridge.mq.host:}")
    private String mqHost;

    @Value("${bridge.mq.port:0}")
    private int mqPort;

    @Value("${bridge.mq.queue-manager:}")
    private String mqQueueManager;

    @Value("${bridge.mq.channel:}")
    private String mqChannel;

    @Value("${bridge.mq.queue:}")
    private String mqQueue;

    @Value("${bridge.mq.username:}")
    private String mqUsername;

    @Value("${bridge.mq.password:}")
    private String mqPassword;

    @Value("${bridge.mq.ssl.enabled:false}")
    private boolean mqSslEnabled;

    @Value("${bridge.mq.ssl.cipher-suite:}")
    private String mqSslCipherSuite;

    @Value("${bridge.kafka.bootstrap-servers:}")
    private String kafkaBootstrapServers;

    @Value("${bridge.kafka.topic:}")
    private String kafkaTopic;

    // Default matches KafkaProperties.securityProtocol — a divergence here would let
    // validation judge a different protocol than the one the producer is built with
    @Value("${bridge.kafka.security-protocol:SASL_SSL}")
    private String kafkaSecurityProtocol;

    @Value("${bridge.kafka.truststore-location:}")
    private String kafkaTruststoreLocation;

    @Value("${bridge.kafka.truststore-password:}")
    private String kafkaTruststorePassword;

    @Value("${bridge.kafka.timeout-seconds:190}")
    private long kafkaPublisherTimeoutSeconds;

    @Value("${bridge.kafka.max-block-ms:60000}")
    private long kafkaMaxBlockMs;

    @Value("${bridge.kafka.delivery-timeout-ms:120000}")
    private long kafkaDeliveryTimeoutMs;

    @Value("${bridge.kafka.acks:all}")
    private String kafkaAcks;

    @Value("${bridge.hdfs.namenode:}")
    private String hdfsNamenode;

    @Value("${bridge.hdfs.base-path:}")
    private String hdfsBasePath;

    @Value("${bridge.hdfs.kerberos.enabled:false}")
    private boolean hdfsKerberosEnabled;

    @Value("${bridge.hdfs.kerberos.principal:}")
    private String hdfsKerberosPrincipal;

    @Value("${bridge.hdfs.kerberos.keytab:}")
    private String hdfsKerberosKeytab;

    @Value("${bridge.api.base-url:}")
    private String apiBaseUrl;

    @Value("${bridge.security.token-url:}")
    private String oauthTokenUrl;

    @Value("${bridge.security.client-id:}")
    private String oauthClientId;

    @Value("${bridge.security.client-secret:}")
    private String oauthClientSecret;

    @Value("${bridge.mq.listener-enabled:false}")
    private boolean mqListenerEnabled;

    @Value("${bridge.mq.require-listener-enabled:false}")
    private boolean requireListenerEnabled;

    @Value("${bridge.validate-only:false}")
    private boolean validateOnly;

    @Value("${bridge.component-test:}")
    private String componentTestMode;

    @Value("${bridge.monitor.enabled:false}")
    private boolean monitorEnabled;

    @Value("${bridge.replay:}")
    private String replayMode;

    @PostConstruct
    public void validateConfiguration() {
        logger.info("=== STARTUP CONFIGURATION VALIDATION ===");

        List<String> errors = new ArrayList<>();
        List<String> warnings = new ArrayList<>();

        validateMqConfig(errors, warnings);
        validateListenerGate(errors, warnings);
        validateKafkaConfig(errors, warnings);
        validateHdfsConfig(errors, warnings);
        validateApiConfig(errors, warnings);
        validateOAuthConfig(errors, warnings);

        logValidationSummary(errors, warnings);

        if (!errors.isEmpty()) {
            String errorMessage = String.format(
                    "Configuration validation failed with %d error(s). " +
                    "Application cannot start. Fix the following:%n%s",
                    errors.size(),
                    String.join("\n", errors)
            );
            logger.error(errorMessage);
            throw new ConfigurationValidationException(errorMessage);
        }

        logger.info("=== CONFIGURATION VALIDATION PASSED ===");
    }

    private void validateMqConfig(List<String> errors, List<String> warnings) {
        mqRules().validateMqConfig(errors, warnings);
    }

    /**
     * Go-live gate: with {@code bridge.mq.require-listener-enabled=true} (set by the prod
     * profile) a launch that forgot {@code --bridge.mq.listener-enabled=true} fails fast
     * instead of running healthy-looking while consuming nothing. The safe-start default
     * of listener-enabled=false is preserved; a deliberate no-consume bring-up passes
     * {@code --bridge.mq.require-listener-enabled=false}. Diagnostic JVMs (validate-only,
     * component-test, monitor) never consume by design and are exempt.
     */
    void validateListenerGate(List<String> errors, List<String> warnings) {
        mqRules().validateListenerGate(errors, warnings);
    }

    private void validateKafkaConfig(List<String> errors, List<String> warnings) {
        new KafkaStartupRules(kafkaBootstrapServers, kafkaSecurityProtocol, kafkaTruststoreLocation,
                kafkaTruststorePassword, kafkaAcks).validateKafkaTransport(errors, warnings);

        // This application publishes claim-check notifications, so it owns the topic it
        // publishes to and the coherence between its publish wait and the producer's budget.
        if (isBlank(kafkaTopic)) {
            errors.add("[KAFKA] bridge.kafka.topic is required");
        } else {
            logger.info("[KAFKA] Topic: {}", kafkaTopic);
        }

        // Timeout coherence: the publisher's future.get() wait must exceed the producer's
        // own budget (metadata max.block + delivery.timeout), or the publisher gives up on
        // records still in flight — which then usually deliver anyway, guaranteeing
        // duplicate-looking redeliveries exactly during broker incidents.
        long producerBudgetMs = kafkaMaxBlockMs + kafkaDeliveryTimeoutMs;
        if (kafkaPublisherTimeoutSeconds * 1000 < producerBudgetMs) {
            warnings.add(String.format(
                    "[KAFKA] bridge.kafka.timeout-seconds (%ds) is less than max-block-ms + "
                            + "delivery-timeout-ms (%dms) - the publisher can time out on "
                            + "records that later deliver, causing duplicates on redelivery",
                    kafkaPublisherTimeoutSeconds, producerBudgetMs));
        } else {
            logger.info("[KAFKA] Publisher wait {}s >= producer budget {}ms (coherent)",
                    kafkaPublisherTimeoutSeconds, producerBudgetMs);
        }
    }

    private void validateHdfsConfig(List<String> errors, List<String> warnings) {
        new HdfsStartupRules(hdfsNamenode, hdfsBasePath, hdfsKerberosEnabled,
                hdfsKerberosPrincipal, hdfsKerberosKeytab).validateHdfsConfig(errors, warnings);
    }

    private void validateApiConfig(List<String> errors, List<String> warnings) {
        logger.info("Validating API configuration...");

        if (isBlank(apiBaseUrl)) {
            errors.add("[API] bridge.api.base-url is required");
        } else {
            if (!apiBaseUrl.startsWith("http://") && !apiBaseUrl.startsWith("https://")) {
                errors.add("[API] bridge.api.base-url must start with http:// or https://");
            } else {
                logger.info("[API] Base URL: {}", apiBaseUrl);
            }
        }
    }

    private void validateOAuthConfig(List<String> errors, List<String> warnings) {
        new StsStartupRules(oauthTokenUrl, oauthClientId, oauthClientSecret)
                .validateOAuthConfig(errors, warnings);
    }

    private void logValidationSummary(List<String> errors, List<String> warnings) {
        logger.info("--- Validation Summary ---");
        logger.info("Errors: {}", errors.size());
        logger.info("Warnings: {}", warnings.size());

        for (String warning : warnings) {
            logger.warn(warning);
        }
    }

    /**
     * The MQ rules, with this application's notion of a diagnostic JVM: validate-only,
     * component-test, monitor and replay modes never consume by design, so the go-live gate
     * does not apply to them.
     */
    private MqStartupRules mqRules() {
        boolean diagnosticMode = validateOnly || monitorEnabled
                || !isBlank(componentTestMode) || !isBlank(replayMode);
        return new MqStartupRules(mqHost, mqPort, mqQueueManager, mqChannel, mqQueue, mqUsername,
                mqPassword, mqSslEnabled, mqSslCipherSuite, mqListenerEnabled, requireListenerEnabled,
                diagnosticMode, " (env: MQ_QUEUE)");
    }

    private boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }

    public static class ConfigurationValidationException extends RuntimeException {
        public ConfigurationValidationException(String message) {
            super(message);
        }
    }
}
