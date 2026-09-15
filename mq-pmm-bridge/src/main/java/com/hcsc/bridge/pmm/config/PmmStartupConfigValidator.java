package com.hcsc.bridge.pmm.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Component;

import com.hcsc.bridge.diagnostics.startup.HdfsStartupRules;
import com.hcsc.bridge.diagnostics.startup.KafkaStartupRules;
import com.hcsc.bridge.diagnostics.startup.MqStartupRules;
import com.hcsc.bridge.diagnostics.startup.StsStartupRules;

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
    // Needed by the shared transport rule: the producer runs with idempotence enabled.
    @Value("${bridge.kafka.acks:all}") private String kafkaAcks;

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
        mqRules().validateMqConfig(errors, warnings);
    }

    /** Go-live gate, identical semantics to the PMM+ bridge; diagnostic JVMs are exempt. */
    void validateListenerGate(List<String> errors, List<String> warnings) {
        mqRules().validateListenerGate(errors, warnings);
    }

    void validateKafkaConfig(List<String> errors, List<String> warnings) {
        // This bridge uses Kafka for the audit stream only, so the transport rules apply
        // exactly when audit events are published to it.
        if (!"kafka".equalsIgnoreCase(auditPublisher)) {
            logger.info("[KAFKA] audit publisher is '{}' - Kafka configuration not required", auditPublisher);
            return;
        }
        new KafkaStartupRules(kafkaBootstrapServers, kafkaSecurityProtocol, kafkaTruststoreLocation,
                kafkaTruststorePassword, kafkaAcks).validateKafkaTransport(errors, warnings);
        if (isBlank(kafkaAuditTopic)) {
            errors.add("[KAFKA] bridge.kafka.audit-topic is required");
        }
    }

    void validateHdfsConfig(List<String> errors, List<String> warnings) {
        new HdfsStartupRules(hdfsNamenode, hdfsBasePath, hdfsKerberosEnabled,
                hdfsKerberosPrincipal, hdfsKerberosKeytab).validateHdfsConfig(errors, warnings);
    }

    void validateOAuthConfig(List<String> errors, List<String> warnings) {
        new StsStartupRules(oauthTokenUrl, oauthClientId, oauthClientSecret)
                .validateOAuthConfig(errors, warnings);
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

    /** The MQ rules, with this application's diagnostic modes exempt from the go-live gate. */
    private MqStartupRules mqRules() {
        boolean diagnosticMode = validateOnly || monitorEnabled;
        return new MqStartupRules(mqHost, mqPort, mqQueueManager, mqChannel, mqQueue, mqUsername,
                mqPassword, mqSslEnabled, mqSslCipherSuite, mqListenerEnabled, requireListenerEnabled,
                diagnosticMode, " (env: PMM_MQ_QUEUE) - the PMM bridge never falls back to the PMM+ queue");
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
