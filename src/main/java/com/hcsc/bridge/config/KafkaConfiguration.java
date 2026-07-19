package com.hcsc.bridge.config;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import javax.annotation.PostConstruct;
import java.io.File;
import java.util.HashMap;
import java.util.Map;

@Configuration
@Profile("!local")
public class KafkaConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(KafkaConfiguration.class);

    private final BridgeProperties.KafkaProperties kafkaProps;
    private final String bootstrapServers;

    public KafkaConfiguration(BridgeProperties bridgeProperties) {
        this.kafkaProps = bridgeProperties.getKafka();
        this.bootstrapServers = kafkaProps.getBootstrapServers();
    }

    @PostConstruct
    public void logSecurityConfiguration() {
        logger.info("=== Kafka Security Configuration ===");
        logger.info("  bootstrap.servers: {}", bootstrapServers);
        logger.info("  security.protocol: {}", kafkaProps.getSecurityProtocol());
        logger.info("  sasl.mechanism: {}", hasValue(kafkaProps.getSaslMechanism()) ? kafkaProps.getSaslMechanism() : "(not set)");
        logger.info("  sasl.kerberos.service.name: {}", hasValue(kafkaProps.getKerberosServiceName()) ? kafkaProps.getKerberosServiceName() : "(not set)");
        logger.info("  ssl.truststore.location: {}", hasValue(kafkaProps.getTruststoreLocation()) ? kafkaProps.getTruststoreLocation() : "(not set)");
        logger.info("  ssl.truststore.type: {}", kafkaProps.getTruststoreType());
        logger.info("  ssl.truststore.password: {}", hasValue(kafkaProps.getTruststorePassword()) ? "********" : "(not set)");
        logger.info("  ssl.keystore.location: {}", hasValue(kafkaProps.getKeystoreLocation()) ? kafkaProps.getKeystoreLocation() : "(not set)");
        logger.info("  jaas.config.path: {}", hasValue(kafkaProps.getJaasConfigPath()) ? kafkaProps.getJaasConfigPath() : "(not set)");
        logger.info("  topic: {}", kafkaProps.getTopic());
        logger.info("  audit-topic: {}", kafkaProps.getAuditTopic());
        logger.info("=====================================");

        validateTruststoreFile();
    }

    private void validateTruststoreFile() {
        String truststorePath = kafkaProps.getTruststoreLocation();
        if (hasValue(truststorePath)) {
            File truststoreFile = new File(truststorePath);
            if (!truststoreFile.exists()) {
                logger.error("SSL truststore file does not exist: {}", truststorePath);
            } else if (!truststoreFile.canRead()) {
                logger.error("SSL truststore file is not readable: {}", truststorePath);
            } else {
                logger.info("SSL truststore file verified: {} ({} bytes)", truststorePath, truststoreFile.length());
            }
        }
    }

    @Bean
    public ProducerFactory<String, String> producerFactory() {
        Map<String, Object> props = buildCommonConfig();

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, kafkaProps.getAcks());
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        // retries stays at the client default (effectively infinite): delivery.timeout.ms is
        // the intended bound for total send time. A small explicit retries value exhausts in
        // under a second of backoff and turns routine broker failovers into publish failures
        // (and, via MQ redelivery, duplicates) long before the delivery timeout is reached.
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, (int) kafkaProps.getDeliveryTimeoutMs());
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, (int) kafkaProps.getRequestTimeoutMs());
        // Bound the time send() itself may block (metadata fetch / full buffer) so the
        // publisher's synchronous wait can be sized to cover the producer's entire budget
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, (int) kafkaProps.getMaxBlockMs());
        props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, kafkaProps.getRequestSize());

        logger.info("Creating Kafka producer factory");
        logSecurityProps(props, "Producer");
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate(ProducerFactory<String, String> producerFactory) {
        KafkaTemplate<String, String> template = new KafkaTemplate<>(producerFactory);
        template.setDefaultTopic(kafkaProps.getTopic());
        return template;
    }

    @Bean
    @Lazy
    public AdminClient kafkaAdminClient() {
        Map<String, Object> props = buildCommonConfig();

        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, (int) kafkaProps.getRequestTimeoutMs());
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, (int) kafkaProps.getDeliveryTimeoutMs());

        logger.info("Creating Kafka AdminClient for readiness checks");
        logSecurityProps(props, "AdminClient");
        return AdminClient.create(props);
    }

    private Map<String, Object> buildCommonConfig() {
        Map<String, Object> props = new HashMap<>();

        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        String securityProtocol = kafkaProps.getSecurityProtocol();
        if (hasValue(securityProtocol)) {
            props.put("security.protocol", securityProtocol);
        }

        addSaslProperties(props);
        addSslProperties(props, securityProtocol);

        return props;
    }

    private void addSaslProperties(Map<String, Object> props) {
        String saslMechanism = kafkaProps.getSaslMechanism();
        if (!hasValue(saslMechanism)) {
            return;
        }

        props.put("sasl.mechanism", saslMechanism);

        String jaasConfigPath = kafkaProps.getJaasConfigPath();
        String saslJaasConfig = kafkaProps.getSaslJaasConfig();

        if (hasValue(jaasConfigPath)) {
            System.setProperty("java.security.auth.login.config", jaasConfigPath);
            // The JVM caches the JAAS Configuration on first use, and Hadoop's
            // Kerberos login earlier in startup may already have initialized it —
            // in which case setting the system property alone is silently ignored.
            // Reset forces a reload that picks up the file above.
            javax.security.auth.login.Configuration.setConfiguration(null);
            logger.info("Using file-based JAAS config: {}", jaasConfigPath);
        } else if (hasValue(saslJaasConfig)) {
            props.put("sasl.jaas.config", saslJaasConfig);
        }

        String kerberosServiceName = kafkaProps.getKerberosServiceName();
        if (hasValue(kerberosServiceName)) {
            props.put("sasl.kerberos.service.name", kerberosServiceName);
        }
    }

    private void addSslProperties(Map<String, Object> props, String securityProtocol) {
        boolean sslEnabled = hasValue(securityProtocol) &&
                (securityProtocol.contains("SSL") || securityProtocol.contains("ssl"));

        if (!sslEnabled) {
            return;
        }

        String truststoreLocation = kafkaProps.getTruststoreLocation();
        if (hasValue(truststoreLocation)) {
            props.put("ssl.truststore.location", truststoreLocation);
            props.put("ssl.truststore.type", kafkaProps.getTruststoreType());

            String truststorePassword = kafkaProps.getTruststorePassword();
            if (hasValue(truststorePassword)) {
                props.put("ssl.truststore.password", truststorePassword);
            }
        }

        String keystoreLocation = kafkaProps.getKeystoreLocation();
        if (hasValue(keystoreLocation)) {
            props.put("ssl.keystore.location", keystoreLocation);
            props.put("ssl.keystore.type", kafkaProps.getTruststoreType());

            String keystorePassword = kafkaProps.getKeystorePassword();
            if (hasValue(keystorePassword)) {
                props.put("ssl.keystore.password", keystorePassword);
            }

            String keyPassword = kafkaProps.getKeyPassword();
            if (hasValue(keyPassword)) {
                props.put("ssl.key.password", keyPassword);
            }
        }
    }

    private void logSecurityProps(Map<String, Object> props, String clientType) {
        logger.info("=== {} SSL/SASL Properties Applied ===", clientType);
        for (Map.Entry<String, Object> entry : props.entrySet()) {
            String key = entry.getKey();
            // sasl.jaas.config may embed credentials inline (PLAIN/SCRAM modules)
            if (key.contains("password") || key.contains("secret") || key.equals("sasl.jaas.config")) {
                logger.info("  {}: ********", key);
            } else if (key.startsWith("ssl.") || key.startsWith("sasl.") || key.equals("security.protocol")) {
                logger.info("  {}: {}", key, entry.getValue());
            }
        }
        logger.info("==========================================");
    }

    private boolean hasValue(String value) {
        return value != null && !value.isEmpty();
    }

    public String getTopic() {
        return kafkaProps.getTopic();
    }

    public String getAuditTopic() {
        return kafkaProps.getAuditTopic();
    }
}
