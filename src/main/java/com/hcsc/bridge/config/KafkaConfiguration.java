package com.hcsc.bridge.config;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import javax.annotation.PreDestroy;
import java.util.HashMap;
import java.util.Map;

@Configuration
@Profile("!local")
public class KafkaConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(KafkaConfiguration.class);

    @Value("${spring.kafka.bootstrap-servers:localhost:9092}")
    private String bootstrapServers;

    @Value("${bridge.kafka.topic:bridge-events}")
    private String topic;

    @Value("${bridge.kafka.audit-topic:bridge-audit}")
    private String auditTopic;

    @Value("${bridge.kafka.delivery-timeout-ms:120000}")
    private int deliveryTimeoutMs;

    @Value("${bridge.kafka.request-timeout-ms:30000}")
    private int requestTimeoutMs;

    @Value("${bridge.kafka.security-protocol:PLAINTEXT}")
    private String securityProtocol;

    @Value("${bridge.kafka.sasl-mechanism:}")
    private String saslMechanism;

    @Value("${bridge.kafka.sasl-jaas-config:}")
    private String saslJaasConfig;

    @Value("${bridge.kafka.kerberos-service-name:}")
    private String kerberosServiceName;

    @Value("${bridge.kafka.jaas-config-path:}")
    private String jaasConfigPath;

    @Value("${bridge.kafka.truststore-location:}")
    private String truststoreLocation;

    @Value("${bridge.kafka.truststore-password:}")
    private String truststorePassword;

    @Value("${bridge.kafka.keystore-location:}")
    private String keystoreLocation;

    @Value("${bridge.kafka.keystore-password:}")
    private String keystorePassword;

    @Value("${bridge.kafka.key-password:}")
    private String keyPassword;

    @Value("${bridge.kafka.request-size:4194400}")
    private int requestSize;

    private AdminClient adminClient;

    @Bean
    public ProducerFactory<String, String> producerFactory() {
        Map<String, Object> props = new HashMap<>();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, deliveryTimeoutMs);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeoutMs);
        props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, requestSize);

        addSecurityProperties(props);

        logger.info("Creating Kafka producer factory with bootstrap servers: {}", bootstrapServers);
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate(ProducerFactory<String, String> producerFactory) {
        KafkaTemplate<String, String> template = new KafkaTemplate<>(producerFactory);
        template.setDefaultTopic(topic);
        return template;
    }

    @Bean
    public AdminClient kafkaAdminClient() {
        Map<String, Object> props = new HashMap<>();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeoutMs);
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, deliveryTimeoutMs);

        addSecurityProperties(props);

        logger.info("Creating Kafka AdminClient for readiness checks");
        this.adminClient = AdminClient.create(props);
        return adminClient;
    }

    private void addSecurityProperties(Map<String, Object> props) {
        if (securityProtocol != null && !securityProtocol.isEmpty() && !securityProtocol.equals("PLAINTEXT")) {
            props.put("security.protocol", securityProtocol);
            logger.debug("Kafka security protocol: {}", securityProtocol);
        }

        if (saslMechanism != null && !saslMechanism.isEmpty()) {
            props.put("sasl.mechanism", saslMechanism);

            // Use file-based JAAS config if path provided (Talend compatibility)
            if (jaasConfigPath != null && !jaasConfigPath.isEmpty()) {
                System.setProperty("java.security.auth.login.config", jaasConfigPath);
                logger.info("Using file-based JAAS config: {}", jaasConfigPath);
            } else if (saslJaasConfig != null && !saslJaasConfig.isEmpty()) {
                props.put("sasl.jaas.config", saslJaasConfig);
            }

            if (kerberosServiceName != null && !kerberosServiceName.isEmpty()) {
                props.put("sasl.kerberos.service.name", kerberosServiceName);
            }
        }

        if (truststoreLocation != null && !truststoreLocation.isEmpty()) {
            props.put("ssl.truststore.location", truststoreLocation);
            if (truststorePassword != null && !truststorePassword.isEmpty()) {
                props.put("ssl.truststore.password", truststorePassword);
            }
        }

        if (keystoreLocation != null && !keystoreLocation.isEmpty()) {
            props.put("ssl.keystore.location", keystoreLocation);
            if (keystorePassword != null && !keystorePassword.isEmpty()) {
                props.put("ssl.keystore.password", keystorePassword);
            }
            if (keyPassword != null && !keyPassword.isEmpty()) {
                props.put("ssl.key.password", keyPassword);
            }
        }
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
        return topic;
    }

    public String getAuditTopic() {
        return auditTopic;
    }
}
