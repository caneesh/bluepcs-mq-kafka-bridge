package com.hcsc.bridge.pmm.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.File;
import java.time.DateTimeException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

/**
 * Fail-fast configuration validation for the PMM bridge, run before the listener can
 * consume. Mirrors the PMM+ bridge's validator for the shared groups (MQ, listener
 * go-live gate, HDFS/Kerberos, STS) and adds the PMM group (API URL, template, XPaths,
 * window). Kafka is validated only when it is actually used, i.e. the audit publisher is
 * {@code kafka}.
 */
@Component
@Profile("!local")
public class PmmStartupConfigValidator {

    private static final Logger logger = LoggerFactory.getLogger(PmmStartupConfigValidator.class);

    @Value("${bridge.mq.host:}") private String mqHost;
    @Value("${bridge.mq.port:0}") private int mqPort;
    @Value("${bridge.mq.queue-manager:}") private String mqQueueManager;
    @Value("${bridge.mq.channel:}") private String mqChannel;
    @Value("${bridge.mq.queue:}") private String mqQueue;
    @Value("${bridge.mq.username:}") private String mqUsername;
    @Value("${bridge.mq.password:}") private String mqPassword;
    @Value("${bridge.mq.ssl.enabled:false}") private boolean mqSslEnabled;
    @Value("${bridge.mq.ssl.cipher-suite:}") private String mqSslCipherSuite;
    @Value("${bridge.mq.listener-enabled:false}") private boolean mqListenerEnabled;
    @Value("${bridge.mq.require-listener-enabled:false}") private boolean requireListenerEnabled;

    @Value("${bridge.audit.publisher:kafka}") private String auditPublisher;
    @Value("${bridge.kafka.bootstrap-servers:}") private String kafkaBootstrapServers;
    @Value("${bridge.kafka.audit-topic:}") private String kafkaAuditTopic;
    @Value("${bridge.kafka.security-protocol:SASL_SSL}") private String kafkaSecurityProtocol;
    @Value("${bridge.kafka.truststore-location:}") private String kafkaTruststoreLocation;
    @Value("${bridge.kafka.truststore-password:}") private String kafkaTruststorePassword;

    @Value("${bridge.hdfs.namenode:}") private String hdfsNamenode;
    @Value("${bridge.hdfs.base-path:}") private String hdfsBasePath;
    @Value("${bridge.hdfs.kerberos.enabled:false}") private boolean hdfsKerberosEnabled;
    @Value("${bridge.hdfs.kerberos.principal:}") private String hdfsKerberosPrincipal;
    @Value("${bridge.hdfs.kerberos.keytab:}") private String hdfsKerberosKeytab;

    @Value("${bridge.security.token-url:}") private String oauthTokenUrl;
    @Value("${bridge.security.client-id:}") private String oauthClientId;
    @Value("${bridge.security.client-secret:}") private String oauthClientSecret;

    @Value("${bridge.pmm.api.url:}") private String pmmApiUrl;
    @Value("${bridge.pmm.api.content-type:application/xml}") private String pmmContentType;
    @Value("${bridge.pmm.template.location:}") private String pmmTemplateLocation;
    @Value("${bridge.pmm.xpath.value1:}") private String pmmXpath1;
    @Value("${bridge.pmm.xpath.value2:}") private String pmmXpath2;
    @Value("${bridge.pmm.hdfs.window-hours:4}") private int windowHours;
    @Value("${bridge.pmm.hdfs.window-zone:UTC}") private String windowZone;

    @Value("${bridge.validate-only:false}") private boolean validateOnly;
    @Value("${bridge.monitor.enabled:false}") private boolean monitorEnabled;

    private final ResourceLoader resourceLoader;

    public PmmStartupConfigValidator(ResourceLoader resourceLoader) {
        this.resourceLoader = resourceLoader;
    }

    @PostConstruct
    public void validateConfiguration() {
        logger.info("=== PMM STARTUP CONFIGURATION VALIDATION ===");
        List<String> errors = new ArrayList<>();
        List<String> warnings = new ArrayList<>();

        validateMqConfig(errors, warnings);
        validateListenerGate(errors, warnings);
        validateKafkaConfig(errors, warnings);
        validateHdfsConfig(errors, warnings);
        validateOAuthConfig(errors, warnings);
        validatePmmConfig(errors, warnings);

        logger.info("--- Validation Summary --- errors: {}, warnings: {}", errors.size(), warnings.size());
        for (String warning : warnings) {
            logger.warn(warning);
        }
        if (!errors.isEmpty()) {
            String message = String.format("Configuration validation failed with %d error(s). "
                    + "Application cannot start. Fix the following:%n%s", errors.size(), String.join("\n", errors));
            logger.error(message);
            throw new ConfigurationValidationException(message);
        }
        logger.info("=== PMM CONFIGURATION VALIDATION PASSED ===");
    }

    void validateMqConfig(List<String> errors, List<String> warnings) {
        if (isBlank(mqHost)) errors.add("[MQ] bridge.mq.host is required");
        if (mqPort < 1 || mqPort > 65535) errors.add("[MQ] bridge.mq.port must be between 1 and 65535");
        if (isBlank(mqQueueManager)) errors.add("[MQ] bridge.mq.queue-manager is required");
        if (isBlank(mqChannel)) errors.add("[MQ] bridge.mq.channel is required");
        if (isBlank(mqQueue)) {
            errors.add("[MQ] bridge.mq.queue is required (env: PMM_MQ_QUEUE) - the PMM bridge never falls back to the PMM+ queue");
        } else {
            logger.info("[MQ] Queue: {} on {}:{} ({}/{})", mqQueue, mqHost, mqPort, mqQueueManager, mqChannel);
        }
        if (isBlank(mqUsername)) {
            warnings.add("[MQ] bridge.mq.username not set - anonymous connection");
        } else if (isBlank(mqPassword)) {
            warnings.add("[MQ] bridge.mq.username set without a password - connecting without MQCSP password authentication");
        }
        if (mqSslEnabled && isBlank(mqSslCipherSuite)) {
            errors.add("[MQ] bridge.mq.ssl.enabled=true requires bridge.mq.ssl.cipher-suite");
        }
    }

    /** Go-live gate, identical semantics to the PMM+ bridge; diagnostic JVMs are exempt. */
    void validateListenerGate(List<String> errors, List<String> warnings) {
        if (validateOnly || monitorEnabled) {
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

    void validateKafkaConfig(List<String> errors, List<String> warnings) {
        if (!"kafka".equalsIgnoreCase(auditPublisher)) {
            logger.info("[KAFKA] audit publisher is '{}' - Kafka configuration not required", auditPublisher);
            return;
        }
        if (isBlank(kafkaBootstrapServers)) errors.add("[KAFKA] bridge.kafka.bootstrap-servers is required for the audit stream");
        if (isBlank(kafkaAuditTopic)) errors.add("[KAFKA] bridge.kafka.audit-topic is required");
        if ("SASL_SSL".equalsIgnoreCase(kafkaSecurityProtocol) || "SSL".equalsIgnoreCase(kafkaSecurityProtocol)) {
            if (isBlank(kafkaTruststoreLocation)) {
                errors.add("[KAFKA] bridge.kafka.truststore-location is required for SSL");
            } else if (!new File(kafkaTruststoreLocation).exists()) {
                errors.add("[KAFKA] Truststore file not found: " + kafkaTruststoreLocation);
            }
            if (isBlank(kafkaTruststorePassword)) {
                errors.add("[KAFKA] bridge.kafka.truststore-password is required for SSL (env: KAFKA_TRUSTSTORE_PASSWORD)");
            }
        }
    }

    void validateHdfsConfig(List<String> errors, List<String> warnings) {
        if (isBlank(hdfsNamenode)) errors.add("[HDFS] bridge.hdfs.namenode is required");
        if (isBlank(hdfsBasePath)) {
            errors.add("[HDFS] bridge.hdfs.base-path is required (env: PMM_HDFS_BASE_PATH)");
        } else {
            logger.info("[HDFS] Landing tree root: {}", hdfsBasePath);
        }
        if (hdfsKerberosEnabled) {
            if (isBlank(hdfsKerberosPrincipal)) {
                errors.add("[HDFS] bridge.hdfs.kerberos.principal is required when Kerberos is enabled");
            }
            if (isBlank(hdfsKerberosKeytab)) {
                errors.add("[HDFS] bridge.hdfs.kerberos.keytab is required when Kerberos is enabled");
            } else {
                File keytab = new File(hdfsKerberosKeytab);
                if (!keytab.exists()) {
                    errors.add("[HDFS] Keytab file not found: " + hdfsKerberosKeytab);
                } else if (!keytab.canRead()) {
                    errors.add("[HDFS] Keytab file not readable: " + hdfsKerberosKeytab);
                }
            }
        }
    }

    void validateOAuthConfig(List<String> errors, List<String> warnings) {
        if (isBlank(oauthTokenUrl)) {
            errors.add("[OAUTH] bridge.security.token-url is required (env: PMM_OAUTH_TOKEN_URL)");
        } else if (!isHttpUrl(oauthTokenUrl)) {
            errors.add("[OAUTH] bridge.security.token-url must start with http:// or https://");
        }
        if (isBlank(oauthClientId)) errors.add("[OAUTH] bridge.security.client-id is required (env: OAUTH_CLIENT_ID)");
        if (isBlank(oauthClientSecret)) errors.add("[OAUTH] bridge.security.client-secret is required (env: OAUTH_CLIENT_SECRET)");
    }

    void validatePmmConfig(List<String> errors, List<String> warnings) {
        if (isBlank(pmmApiUrl)) {
            errors.add("[PMM] bridge.pmm.api.url is required (env: PMM_API_URL)");
        } else if (!isHttpUrl(pmmApiUrl)) {
            errors.add("[PMM] bridge.pmm.api.url must start with http:// or https://");
        } else {
            logger.info("[PMM] API URL: {} ({})", pmmApiUrl, pmmContentType);
        }
        if (isBlank(pmmContentType)) {
            errors.add("[PMM] bridge.pmm.api.content-type must not be blank");
        }

        if (isBlank(pmmTemplateLocation)) {
            errors.add("[PMM] bridge.pmm.template.location is required (env: PMM_TEMPLATE_LOCATION)");
        } else {
            Resource template = resourceLoader.getResource(pmmTemplateLocation.trim());
            if (!template.exists()) {
                errors.add("[PMM] request template not found: " + pmmTemplateLocation);
            } else if (!template.isReadable()) {
                errors.add("[PMM] request template not readable: " + pmmTemplateLocation);
            } else {
                logger.info("[PMM] Request template: {}", pmmTemplateLocation);
            }
        }

        validateXpath(errors, "bridge.pmm.xpath.value1", "PMM_XPATH_VALUE1", pmmXpath1);
        validateXpath(errors, "bridge.pmm.xpath.value2", "PMM_XPATH_VALUE2", pmmXpath2);

        if (windowHours < 1 || windowHours > 24 || 24 % windowHours != 0) {
            errors.add("[PMM] bridge.pmm.hdfs.window-hours must divide 24 evenly, got " + windowHours);
        }
        try {
            ZoneId.of(isBlank(windowZone) ? "UTC" : windowZone.trim());
        } catch (DateTimeException e) {
            errors.add("[PMM] bridge.pmm.hdfs.window-zone is not a valid zone id: " + windowZone);
        }
    }

    private void validateXpath(List<String> errors, String key, String env, String expression) {
        if (isBlank(expression)) {
            errors.add("[PMM] " + key + " is required (env: " + env + ")");
            return;
        }
        try {
            XPathFactory.newInstance().newXPath().compile(expression.trim());
        } catch (XPathExpressionException e) {
            errors.add("[PMM] " + key + " is not a valid XPath expression: " + expression + " (" + e.getMessage() + ")");
        }
    }

    private static boolean isHttpUrl(String url) {
        return url.startsWith("http://") || url.startsWith("https://");
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }

    public static class ConfigurationValidationException extends RuntimeException {
        public ConfigurationValidationException(String message) {
            super(message);
        }
    }
}
