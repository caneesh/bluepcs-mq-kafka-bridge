package com.enterprise.bridge.config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.test.util.ReflectionTestUtils;

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
            assertThat(props.get(ProducerConfig.RETRIES_CONFIG)).isEqualTo(Integer.MAX_VALUE);
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
            KafkaConfiguration config = createConfiguration();
            ReflectionTestUtils.setField(config, "deliveryTimeoutMs", 60000);
            ReflectionTestUtils.setField(config, "requestTimeoutMs", 15000);

            ProducerFactory<String, String> factory = config.producerFactory();
            Map<String, Object> props = ((DefaultKafkaProducerFactory<String, String>) factory).getConfigurationProperties();

            assertThat(props.get(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG)).isEqualTo(60000);
            assertThat(props.get(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG)).isEqualTo(15000);
        }

        @Test
        @DisplayName("should configure bootstrap servers")
        void shouldConfigureBootstrapServers() {
            KafkaConfiguration config = createConfiguration();
            ReflectionTestUtils.setField(config, "bootstrapServers", "kafka1:9092,kafka2:9092");

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
            KafkaConfiguration config = createConfiguration();
            ReflectionTestUtils.setField(config, "securityProtocol", "SASL_SSL");
            ReflectionTestUtils.setField(config, "saslMechanism", "SCRAM-SHA-512");
            ReflectionTestUtils.setField(config, "saslJaasConfig",
                    "org.apache.kafka.common.security.scram.ScramLoginModule required;");

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
            KafkaConfiguration config = createConfiguration();
            ReflectionTestUtils.setField(config, "securityProtocol", "SASL_PLAINTEXT");
            ReflectionTestUtils.setField(config, "saslMechanism", "GSSAPI");
            ReflectionTestUtils.setField(config, "kerberosServiceName", "kafka");

            ProducerFactory<String, String> factory = config.producerFactory();
            Map<String, Object> props = ((DefaultKafkaProducerFactory<String, String>) factory).getConfigurationProperties();

            assertThat(props.get("security.protocol")).isEqualTo("SASL_PLAINTEXT");
            assertThat(props.get("sasl.mechanism")).isEqualTo("GSSAPI");
            assertThat(props.get("sasl.kerberos.service.name")).isEqualTo("kafka");
        }

        @Test
        @DisplayName("should configure SSL truststore")
        void shouldConfigureSslTruststore() {
            KafkaConfiguration config = createConfiguration();
            ReflectionTestUtils.setField(config, "securityProtocol", "SSL");
            ReflectionTestUtils.setField(config, "truststoreLocation", "/path/to/truststore.jks");
            ReflectionTestUtils.setField(config, "truststorePassword", "truststore-pass");

            ProducerFactory<String, String> factory = config.producerFactory();
            Map<String, Object> props = ((DefaultKafkaProducerFactory<String, String>) factory).getConfigurationProperties();

            assertThat(props.get("security.protocol")).isEqualTo("SSL");
            assertThat(props.get("ssl.truststore.location")).isEqualTo("/path/to/truststore.jks");
            assertThat(props.get("ssl.truststore.password")).isEqualTo("truststore-pass");
        }

        @Test
        @DisplayName("should configure SSL keystore")
        void shouldConfigureSslKeystore() {
            KafkaConfiguration config = createConfiguration();
            ReflectionTestUtils.setField(config, "securityProtocol", "SSL");
            ReflectionTestUtils.setField(config, "keystoreLocation", "/path/to/keystore.jks");
            ReflectionTestUtils.setField(config, "keystorePassword", "keystore-pass");
            ReflectionTestUtils.setField(config, "keyPassword", "key-pass");

            ProducerFactory<String, String> factory = config.producerFactory();
            Map<String, Object> props = ((DefaultKafkaProducerFactory<String, String>) factory).getConfigurationProperties();

            assertThat(props.get("ssl.keystore.location")).isEqualTo("/path/to/keystore.jks");
            assertThat(props.get("ssl.keystore.password")).isEqualTo("keystore-pass");
            assertThat(props.get("ssl.key.password")).isEqualTo("key-pass");
        }

        @Test
        @DisplayName("should not add security properties for PLAINTEXT")
        void shouldNotAddSecurityForPlaintext() {
            KafkaConfiguration config = createConfiguration();
            ReflectionTestUtils.setField(config, "securityProtocol", "PLAINTEXT");

            ProducerFactory<String, String> factory = config.producerFactory();
            Map<String, Object> props = ((DefaultKafkaProducerFactory<String, String>) factory).getConfigurationProperties();

            assertThat(props.containsKey("security.protocol")).isFalse();
            assertThat(props.containsKey("sasl.mechanism")).isFalse();
        }
    }

    @Nested
    @DisplayName("Topic Configuration")
    class TopicConfigurationTests {

        @Test
        @DisplayName("should expose topic name")
        void shouldExposeTopicName() {
            KafkaConfiguration config = createConfiguration();
            ReflectionTestUtils.setField(config, "topic", "custom-topic");

            assertThat(config.getTopic()).isEqualTo("custom-topic");
        }

        @Test
        @DisplayName("should expose audit topic name")
        void shouldExposeAuditTopicName() {
            KafkaConfiguration config = createConfiguration();
            ReflectionTestUtils.setField(config, "auditTopic", "audit-events");

            assertThat(config.getAuditTopic()).isEqualTo("audit-events");
        }
    }

    private KafkaConfiguration createConfiguration() {
        KafkaConfiguration config = new KafkaConfiguration();
        ReflectionTestUtils.setField(config, "bootstrapServers", "localhost:9092");
        ReflectionTestUtils.setField(config, "topic", "test-topic");
        ReflectionTestUtils.setField(config, "auditTopic", "test-audit");
        ReflectionTestUtils.setField(config, "deliveryTimeoutMs", 120000);
        ReflectionTestUtils.setField(config, "requestTimeoutMs", 30000);
        ReflectionTestUtils.setField(config, "securityProtocol", "PLAINTEXT");
        ReflectionTestUtils.setField(config, "saslMechanism", "");
        ReflectionTestUtils.setField(config, "saslJaasConfig", "");
        ReflectionTestUtils.setField(config, "kerberosServiceName", "");
        ReflectionTestUtils.setField(config, "truststoreLocation", "");
        ReflectionTestUtils.setField(config, "truststorePassword", "");
        ReflectionTestUtils.setField(config, "keystoreLocation", "");
        ReflectionTestUtils.setField(config, "keystorePassword", "");
        ReflectionTestUtils.setField(config, "keyPassword", "");
        return config;
    }
}
