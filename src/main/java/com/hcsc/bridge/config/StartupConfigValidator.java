package com.hcsc.bridge.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

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

    @Value("${bridge.kafka.bootstrap-servers:}")
    private String kafkaBootstrapServers;

    @Value("${bridge.kafka.topic:}")
    private String kafkaTopic;

    @Value("${bridge.kafka.security-protocol:PLAINTEXT}")
    private String kafkaSecurityProtocol;

    @Value("${bridge.kafka.truststore-location:}")
    private String kafkaTruststoreLocation;

    @Value("${bridge.kafka.truststore-password:}")
    private String kafkaTruststorePassword;

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

    @PostConstruct
    public void validateConfiguration() {
        logger.info("=== STARTUP CONFIGURATION VALIDATION ===");

        List<String> errors = new ArrayList<>();
        List<String> warnings = new ArrayList<>();

        validateMqConfig(errors, warnings);
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
            errors.add("[MQ] bridge.mq.queue is required");
        } else {
            logger.info("[MQ] Queue: {}", mqQueue);
        }

        if (isBlank(mqUsername)) {
            warnings.add("[MQ] bridge.mq.username not set - anonymous connection");
        } else {
            logger.info("[MQ] Username: {}", mqUsername);
        }

        if (isBlank(mqPassword) && !isBlank(mqUsername)) {
            errors.add("[MQ] bridge.mq.password is required when username is set (env: MQ_PASSWORD)");
        } else if (!isBlank(mqPassword)) {
            logger.info("[MQ] Password: ********");
        }
    }

    private void validateKafkaConfig(List<String> errors, List<String> warnings) {
        logger.info("Validating Kafka configuration...");

        if (isBlank(kafkaBootstrapServers)) {
            errors.add("[KAFKA] bridge.kafka.bootstrap-servers is required");
        } else {
            logger.info("[KAFKA] Bootstrap Servers: {}", kafkaBootstrapServers);
        }

        if (isBlank(kafkaTopic)) {
            errors.add("[KAFKA] bridge.kafka.topic is required");
        } else {
            logger.info("[KAFKA] Topic: {}", kafkaTopic);
        }

        logger.info("[KAFKA] Security Protocol: {}", kafkaSecurityProtocol);

        if ("SASL_SSL".equals(kafkaSecurityProtocol) || "SSL".equals(kafkaSecurityProtocol)) {
            if (isBlank(kafkaTruststoreLocation)) {
                errors.add("[KAFKA] bridge.kafka.truststore-location is required for SSL");
            } else {
                File truststoreFile = new File(kafkaTruststoreLocation);
                if (!truststoreFile.exists()) {
                    errors.add("[KAFKA] Truststore file not found: " + kafkaTruststoreLocation);
                } else {
                    logger.info("[KAFKA] Truststore: {} (exists)", kafkaTruststoreLocation);
                }
            }

            if (isBlank(kafkaTruststorePassword)) {
                errors.add("[KAFKA] bridge.kafka.truststore-password is required for SSL (env: KAFKA_TRUSTSTORE_PASSWORD)");
            } else {
                logger.info("[KAFKA] Truststore Password: ********");
            }
        }
    }

    private void validateHdfsConfig(List<String> errors, List<String> warnings) {
        logger.info("Validating HDFS configuration...");

        if (isBlank(hdfsNamenode)) {
            errors.add("[HDFS] bridge.hdfs.namenode is required");
        } else {
            logger.info("[HDFS] Namenode: {}", hdfsNamenode);
        }

        if (isBlank(hdfsBasePath)) {
            errors.add("[HDFS] bridge.hdfs.base-path is required");
        } else {
            logger.info("[HDFS] Base Path: {}", hdfsBasePath);
        }

        if (hdfsKerberosEnabled) {
            logger.info("[HDFS] Kerberos: ENABLED");

            if (isBlank(hdfsKerberosPrincipal)) {
                errors.add("[HDFS] bridge.hdfs.kerberos.principal is required when Kerberos is enabled");
            } else {
                logger.info("[HDFS] Kerberos Principal: {}", hdfsKerberosPrincipal);
            }

            if (isBlank(hdfsKerberosKeytab)) {
                errors.add("[HDFS] bridge.hdfs.kerberos.keytab is required when Kerberos is enabled");
            } else {
                File keytabFile = new File(hdfsKerberosKeytab);
                if (!keytabFile.exists()) {
                    errors.add("[HDFS] Keytab file not found: " + hdfsKerberosKeytab);
                } else if (!keytabFile.canRead()) {
                    errors.add("[HDFS] Keytab file not readable: " + hdfsKerberosKeytab);
                } else {
                    logger.info("[HDFS] Keytab: {} (exists, readable)", hdfsKerberosKeytab);
                }
            }
        } else {
            logger.info("[HDFS] Kerberos: DISABLED");
        }
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
        logger.info("Validating OAuth configuration...");

        if (isBlank(oauthTokenUrl)) {
            errors.add("[OAUTH] bridge.security.token-url is required");
        } else {
            if (!oauthTokenUrl.startsWith("http://") && !oauthTokenUrl.startsWith("https://")) {
                errors.add("[OAUTH] bridge.security.token-url must start with http:// or https://");
            } else {
                logger.info("[OAUTH] Token URL: {}", oauthTokenUrl);
            }
        }

        if (isBlank(oauthClientId)) {
            errors.add("[OAUTH] bridge.security.client-id is required");
        } else {
            logger.info("[OAUTH] Client ID: {}", oauthClientId);
        }

        if (isBlank(oauthClientSecret)) {
            errors.add("[OAUTH] bridge.security.client-secret is required (env: OAUTH_CLIENT_SECRET)");
        } else {
            logger.info("[OAUTH] Client Secret: ********");
        }
    }

    private void logValidationSummary(List<String> errors, List<String> warnings) {
        logger.info("--- Validation Summary ---");
        logger.info("Errors: {}", errors.size());
        logger.info("Warnings: {}", warnings.size());

        for (String warning : warnings) {
            logger.warn(warning);
        }
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
