package com.hcsc.bridge.pmm.config;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.test.util.ReflectionTestUtils;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("PmmStartupConfigValidator")
class PmmStartupConfigValidatorTest {

    private PmmStartupConfigValidator validator;

    @BeforeEach
    void setUp() {
        validator = new PmmStartupConfigValidator(new DefaultResourceLoader());
        set("mqHost", "mq.example"); set("mqPort", 1414); set("mqQueueManager", "QM1");
        set("mqChannel", "CH"); set("mqQueue", "PMM.Q"); set("mqUsername", "u"); set("mqPassword", "p");
        set("mqListenerEnabled", true); set("requireListenerEnabled", true);
        set("auditPublisher", "log");
        set("kafkaSecurityProtocol", "PLAINTEXT");
        set("hdfsNamenode", "hdfs://nn"); set("hdfsBasePath", "/data/pmm"); set("hdfsKerberosEnabled", false);
        set("oauthTokenUrl", "https://sts.example/token"); set("oauthClientId", "id"); set("oauthClientSecret", "s");
        set("pmmApiUrl", "https://api.example/pmm"); set("pmmContentType", "application/xml");
        set("pmmTemplateLocation", "classpath:pmm/request-template.sample.xml");
        set("pmmXpath1", "/a/b"); set("pmmXpath2", "/a/c");
        set("windowHours", 4); set("windowZone", "UTC");
    }

    private void set(String field, Object value) {
        ReflectionTestUtils.setField(validator, field, value);
    }

    @Test
    @DisplayName("passes with a complete configuration")
    void passes() {
        assertThatCode(validator::validateConfiguration).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("requires the PMM queue and names the environment variable")
    void requiresQueue() {
        set("mqQueue", "");
        assertThatThrownBy(validator::validateConfiguration)
                .isInstanceOf(PmmStartupConfigValidator.ConfigurationValidationException.class)
                .hasMessageContaining("PMM_MQ_QUEUE");
    }

    @Test
    @DisplayName("rejects an invalid XPath, a bad window size and a bad zone")
    void rejectsBadPmmConfig() {
        set("pmmXpath2", "/a["); set("windowHours", 5); set("windowZone", "Mars/Olympus");
        assertThatThrownBy(validator::validateConfiguration)
                .hasMessageContaining("bridge.pmm.xpath.value2")
                .hasMessageContaining("window-hours")
                .hasMessageContaining("window-zone");
    }

    @Test
    @DisplayName("rejects a missing template and a non-http API URL")
    void rejectsTemplateAndUrl() {
        set("pmmTemplateLocation", "file:/nope/none.xml"); set("pmmApiUrl", "ftp://x");
        assertThatThrownBy(validator::validateConfiguration)
                .hasMessageContaining("template not found")
                .hasMessageContaining("http://");
    }

    @Test
    @DisplayName("enforces the go-live gate except in diagnostic modes")
    void listenerGate() {
        set("mqListenerEnabled", false);
        assertThatThrownBy(validator::validateConfiguration).hasMessageContaining("require-listener-enabled");
        set("validateOnly", true);
        assertThatCode(validator::validateConfiguration).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("validates Kafka only when the audit publisher is kafka")
    void kafkaOnlyForKafkaAudit() {
        set("kafkaBootstrapServers", ""); set("kafkaAuditTopic", "");
        assertThatCode(validator::validateConfiguration).doesNotThrowAnyException();
        set("auditPublisher", "kafka");
        assertThatThrownBy(validator::validateConfiguration)
                .hasMessageContaining("bootstrap-servers").hasMessageContaining("audit-topic");
    }
}
