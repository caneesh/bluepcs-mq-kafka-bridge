package com.hcsc.bridge.config;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.File;
import java.util.HashMap;
import java.util.Map;

@Configuration
@Profile("!local")
public class KafkaConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(KafkaConfiguration.class);

    private final BridgeProperties.KafkaProperties kafkaProps;
    private final String bootstrapServers;

    private AdminClient adminClient;

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
        props.put(ProducerConfig.RETRIES_CONFIG, kafkaProps.getRetries());
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, (int) kafkaProps.getDeliveryTimeoutMs());
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, (int) kafkaProps.getRequestTimeoutMs());
        props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, kafkaProps.getRequestSize());

        logger.info("Creating Kafka producer factory");
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate(ProducerFactory<String, String> producerFactory) {
        KafkaTemplate<String, String> template = new KafkaTemplate<>(producerFactory);
        template.setDefaultTopic(kafkaProps.getTopic());
        return template;
    }

    @Bean
    public AdminClient kafkaAdminClient() {
        Map<String, Object> props = buildCommonConfig();

        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, (int) kafkaProps.getRequestTimeoutMs());
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, (int) kafkaProps.getDeliveryTimeoutMs());

        logger.info("Creating Kafka AdminClient for readiness checks");
        this.adminClient = AdminClient.create(props);
        return adminClient;
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
            logger.debug("Using file-based JAAS config: {}", jaasConfigPath);
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

    private boolean hasValue(String value) {
        return value != null && !value.isEmpty();
    }

    @PreDestroy
    public void cleanup() {
        if (adminClient != null) {
            try {
                adminClient.close();
                logger.info("Kafka AdminClient closed");
            } catch (Exception e) {
                logger.warn("Error closing Kafka AdminClient", e);
            }
        }
    }

    public String getTopic() {
        return kafkaProps.getTopic();
    }

    public String getAuditTopic() {
        return kafkaProps.getAuditTopic();
    }
}
