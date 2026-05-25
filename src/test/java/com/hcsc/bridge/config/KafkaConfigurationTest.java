package com.hcsc.bridge.config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.ProducerFactory;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaConfigurationTest {

    @Nested
    @DisplayName("Producer Factory Configuration")
    class ProducerFactoryTests {

        @Test
        @DisplayName("should configure idempotent producer settings")
        void shouldConfigureIdempotentSettings() {
            KafkaConfiguration config = createConfiguration();

            ProducerFactory<String, String> factory = config.producerFactory();

            assertThat(factory).isInstanceOf(DefaultKafkaProducerFactory.class);
            Map<String, Object> props = ((DefaultKafkaProducerFactory<String, String>) factory).getConfigurationProperties();

            assertThat(props.get(ProducerConfig.ACKS_CONFIG)).isEqualTo("all");
            assertThat(props.get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG)).isEqualTo(true);
            assertThat(props.get(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION)).isEqualTo(5);
        }

        @Test
        @DisplayName("should configure compression")
        void shouldConfigureCompression() {
            KafkaConfiguration config = createConfiguration();

            ProducerFactory<String, String> factory = config.producerFactory();
            Map<String, Object> props = ((DefaultKafkaProducerFactory<String, String>) factory).getConfigurationProperties();

            assertThat(props.get(ProducerConfig.COMPRESSION_TYPE_CONFIG)).isEqualTo("snappy");
        }

        @Test
        @DisplayName("should configure timeout settings")
        void shouldConfigureTimeouts() {
            BridgeProperties.KafkaProperties kafkaProps = createKafkaProperties();
            kafkaProps.setDeliveryTimeoutMs(60000);
            kafkaProps.setRequestTimeoutMs(15000);
            KafkaConfiguration config = createConfiguration(kafkaProps);

            ProducerFactory<String, String> factory = config.producerFactory();
            Map<String, Object> props = ((DefaultKafkaProducerFactory<String, String>) factory).getConfigurationProperties();

            assertThat(props.get(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG)).isEqualTo(60000);
            assertThat(props.get(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG)).isEqualTo(15000);
        }

        @Test
        @DisplayName("should configure bootstrap servers")
        void shouldConfigureBootstrapServers() {
            BridgeProperties.KafkaProperties kafkaProps = createKafkaProperties();
            kafkaProps.setBootstrapServers("kafka1:9092,kafka2:9092");
            KafkaConfiguration config = createConfiguration(kafkaProps);

            ProducerFactory<String, String> factory = config.producerFactory();
            Map<String, Object> props = ((DefaultKafkaProducerFactory<String, String>) factory).getConfigurationProperties();

            assertThat(props.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG)).isEqualTo("kafka1:9092,kafka2:9092");
        }
    }

    @Nested
    @DisplayName("Security Configuration")
    class SecurityConfigurationTests {

        @Test
        @DisplayName("should configure SASL_SSL security")
        void shouldConfigureSaslSsl() {
            BridgeProperties.KafkaProperties kafkaProps = createKafkaProperties();
            kafkaProps.setSecurityProtocol("SASL_SSL");
            kafkaProps.setSaslMechanism("SCRAM-SHA-512");
            kafkaProps.setSaslJaasConfig("org.apache.kafka.common.security.scram.ScramLoginModule required;");
            kafkaProps.setTruststoreLocation("/path/to/truststore.jks");
            kafkaProps.setTruststorePassword("trust-pass");
            KafkaConfiguration config = createConfiguration(kafkaProps);

            ProducerFactory<String, String> factory = config.producerFactory();
            Map<String, Object> props = ((DefaultKafkaProducerFactory<String, String>) factory).getConfigurationProperties();

            assertThat(props.get("security.protocol")).isEqualTo("SASL_SSL");
            assertThat(props.get("sasl.mechanism")).isEqualTo("SCRAM-SHA-512");
            assertThat(props.get("sasl.jaas.config"))
                    .isEqualTo("org.apache.kafka.common.security.scram.ScramLoginModule required;");
        }

        @Test
        @DisplayName("should configure Kerberos authentication")
        void shouldConfigureKerberos() {
            BridgeProperties.KafkaProperties kafkaProps = createKafkaProperties();
            kafkaProps.setSecurityProtocol("SASL_PLAINTEXT");
            kafkaProps.setSaslMechanism("GSSAPI");
            kafkaProps.setKerberosServiceName("kafka");
            KafkaConfiguration config = createConfiguration(kafkaProps);

            ProducerFactory<String, String> factory = config.producerFactory();
            Map<String, Object> props = ((DefaultKafkaProducerFactory<String, String>) factory).getConfigurationProperties();

            assertThat(props.get("security.protocol")).isEqualTo("SASL_PLAINTEXT");
            assertThat(props.get("sasl.mechanism")).isEqualTo("GSSAPI");
            assertThat(props.get("sasl.kerberos.service.name")).isEqualTo("kafka");
        }

        @Test
        @DisplayName("should configure SSL truststore with type")
        void shouldConfigureSslTruststore() {
            BridgeProperties.KafkaProperties kafkaProps = createKafkaProperties();
            kafkaProps.setSecurityProtocol("SSL");
            kafkaProps.setTruststoreLocation("/path/to/truststore.jks");
            kafkaProps.setTruststorePassword("truststore-pass");
            kafkaProps.setTruststoreType("JKS");
            KafkaConfiguration config = createConfiguration(kafkaProps);

            ProducerFactory<String, String> factory = config.producerFactory();
            Map<String, Object> props = ((DefaultKafkaProducerFactory<String, String>) factory).getConfigurationProperties();

            assertThat(props.get("security.protocol")).isEqualTo("SSL");
            assertThat(props.get("ssl.truststore.location")).isEqualTo("/path/to/truststore.jks");
            assertThat(props.get("ssl.truststore.password")).isEqualTo("truststore-pass");
            assertThat(props.get("ssl.truststore.type")).isEqualTo("JKS");
        }

        @Test
        @DisplayName("should configure SSL keystore")
        void shouldConfigureSslKeystore() {
            BridgeProperties.KafkaProperties kafkaProps = createKafkaProperties();
            kafkaProps.setSecurityProtocol("SSL");
            kafkaProps.setTruststoreLocation("/path/to/truststore.jks");
            kafkaProps.setKeystoreLocation("/path/to/keystore.jks");
            kafkaProps.setKeystorePassword("keystore-pass");
            kafkaProps.setKeyPassword("key-pass");
            KafkaConfiguration config = createConfiguration(kafkaProps);

            ProducerFactory<String, String> factory = config.producerFactory();
            Map<String, Object> props = ((DefaultKafkaProducerFactory<String, String>) factory).getConfigurationProperties();

            assertThat(props.get("ssl.keystore.location")).isEqualTo("/path/to/keystore.jks");
            assertThat(props.get("ssl.keystore.password")).isEqualTo("keystore-pass");
            assertThat(props.get("ssl.key.password")).isEqualTo("key-pass");
        }

        @Test
        @DisplayName("should not add SSL properties for PLAINTEXT")
        void shouldNotAddSslForPlaintext() {
            BridgeProperties.KafkaProperties kafkaProps = createKafkaProperties();
            kafkaProps.setSecurityProtocol("PLAINTEXT");
            kafkaProps.setTruststoreLocation("/path/to/truststore.jks");
            KafkaConfiguration config = createConfiguration(kafkaProps);

            ProducerFactory<String, String> factory = config.producerFactory();
            Map<String, Object> props = ((DefaultKafkaProducerFactory<String, String>) factory).getConfigurationProperties();

            assertThat(props.containsKey("ssl.truststore.location")).isFalse();
        }
    }

    @Nested
    @DisplayName("Topic Configuration")
    class TopicConfigurationTests {

        @Test
        @DisplayName("should expose topic name")
        void shouldExposeTopicName() {
            BridgeProperties.KafkaProperties kafkaProps = createKafkaProperties();
            kafkaProps.setTopic("custom-topic");
            KafkaConfiguration config = createConfiguration(kafkaProps);

            assertThat(config.getTopic()).isEqualTo("custom-topic");
        }

        @Test
        @DisplayName("should expose audit topic name")
        void shouldExposeAuditTopicName() {
            BridgeProperties.KafkaProperties kafkaProps = createKafkaProperties();
            kafkaProps.setAuditTopic("audit-events");
            KafkaConfiguration config = createConfiguration(kafkaProps);

            assertThat(config.getAuditTopic()).isEqualTo("audit-events");
        }
    }

    private KafkaConfiguration createConfiguration() {
        return createConfiguration(createKafkaProperties());
    }

    private KafkaConfiguration createConfiguration(BridgeProperties.KafkaProperties kafkaProps) {
        BridgeProperties bridgeProperties = new BridgeProperties();
        bridgeProperties.setKafka(kafkaProps);
        return new KafkaConfiguration(bridgeProperties);
    }

    private BridgeProperties.KafkaProperties createKafkaProperties() {
        BridgeProperties.KafkaProperties props = new BridgeProperties.KafkaProperties();
        props.setBootstrapServers("localhost:9092");
        props.setTopic("test-topic");
        props.setAuditTopic("test-audit");
        props.setDeliveryTimeoutMs(120000);
        props.setRequestTimeoutMs(30000);
        props.setSecurityProtocol("PLAINTEXT");
        props.setAcks("all");
        props.setRetries(5);
        props.setTruststoreType("JKS");
        return props;
    }
}
