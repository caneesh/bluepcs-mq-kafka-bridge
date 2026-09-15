package com.hcsc.bridge.diagnostics.startup;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The startup rules both bridges share. They live here, tested once, because the two
 * applications used to re-implement them and the copies had drifted: the second bridge
 * checked fewer things than the first, so the same misconfiguration failed fast in one
 * process and started happily in the other.
 */
@DisplayName("shared startup rules")
class SharedStartupRulesTest {

    private final List<String> errors = new ArrayList<>();
    private final List<String> warnings = new ArrayList<>();

    @Nested
    @DisplayName("MQ")
    class Mq {

        private MqStartupRules rules(String host, int port, String queue, boolean sslEnabled, String cipher) {
            return new MqStartupRules(host, port, "QM1", "CH", queue, "user", "pass",
                    sslEnabled, cipher, true, true, false, " (env: MQ_QUEUE)");
        }

        @Test
        @DisplayName("requires host, a valid port, and the queue, naming the application's env var")
        void requiresConnectionDetails() {
            rules("", 0, "", false, "").validateMqConfig(errors, warnings);

            assertThat(errors).anyMatch(e -> e.contains("bridge.mq.host"));
            assertThat(errors).anyMatch(e -> e.contains("port"));
            assertThat(errors).anyMatch(e -> e.contains("bridge.mq.queue") && e.contains("MQ_QUEUE"));
        }

        @Test
        @DisplayName("rejects TLS without a cipher suite")
        void rejectsSslWithoutCipher() {
            rules("mq.example", 1414, "Q", true, "").validateMqConfig(errors, warnings);

            assertThat(errors).anyMatch(e -> e.contains("cipher-suite"));
        }

        @Test
        @DisplayName("accepts a complete configuration")
        void acceptsComplete() {
            rules("mq.example", 1414, "Q", false, "").validateMqConfig(errors, warnings);

            assertThat(errors).isEmpty();
        }

        @Test
        @DisplayName("the go-live gate fails a non-consuming start, and exempts diagnostic JVMs")
        void goLiveGate() {
            new MqStartupRules("h", 1414, "QM", "CH", "Q", "u", "p", false, "",
                    false, true, false, "").validateListenerGate(errors, warnings);
            assertThat(errors).anyMatch(e -> e.contains("require-listener-enabled"));

            errors.clear();
            new MqStartupRules("h", 1414, "QM", "CH", "Q", "u", "p", false, "",
                    false, true, true, "").validateListenerGate(errors, warnings);
            assertThat(errors).isEmpty();
        }

        @Test
        @DisplayName("a deliberate safe start warns rather than fails")
        void safeStartWarns() {
            new MqStartupRules("h", 1414, "QM", "CH", "Q", "u", "p", false, "",
                    false, false, false, "").validateListenerGate(errors, warnings);

            assertThat(errors).isEmpty();
            assertThat(warnings).anyMatch(w -> w.contains("safe-start"));
        }
    }

    @Nested
    @DisplayName("Kafka transport")
    class Kafka {

        @Test
        @DisplayName("requires acks=all because the producer runs with idempotence enabled")
        void requiresAcksAll() {
            new KafkaStartupRules("broker:9093", "PLAINTEXT", "", "", "1")
                    .validateKafkaTransport(errors, warnings);

            assertThat(errors).anyMatch(e -> e.contains("acks=1") && e.contains("idempotence"));
        }

        @Test
        @DisplayName("requires a truststore that exists, plus its password, for TLS")
        void requiresTruststoreMaterial(@TempDir Path dir) throws Exception {
            new KafkaStartupRules("broker:9093", "SASL_SSL", "/nope/missing.jks", "", "all")
                    .validateKafkaTransport(errors, warnings);
            assertThat(errors).anyMatch(e -> e.contains("Truststore file not found"));
            assertThat(errors).anyMatch(e -> e.contains("truststore-password"));

            errors.clear();
            Path jks = Files.createFile(dir.resolve("kafka.jks"));
            new KafkaStartupRules("broker:9093", "SASL_SSL", jks.toString(), "secret", "all")
                    .validateKafkaTransport(errors, warnings);
            assertThat(errors).isEmpty();
        }

        @Test
        @DisplayName("requires bootstrap servers")
        void requiresBootstrap() {
            new KafkaStartupRules("", "PLAINTEXT", "", "", "all").validateKafkaTransport(errors, warnings);

            assertThat(errors).anyMatch(e -> e.contains("bootstrap-servers"));
        }
    }

    @Nested
    @DisplayName("HDFS")
    class Hdfs {

        @Test
        @DisplayName("requires namenode and base path")
        void requiresLocation() {
            new HdfsStartupRules("", "", false, "", "").validateHdfsConfig(errors, warnings);

            assertThat(errors).anyMatch(e -> e.contains("namenode"));
            assertThat(errors).anyMatch(e -> e.contains("base-path"));
        }

        @Test
        @DisplayName("with Kerberos on, requires a principal and a keytab that exists and is readable")
        void requiresKeytab(@TempDir Path dir) throws Exception {
            new HdfsStartupRules("hdfs://nn", "/data", true, "", "/nope/missing.keytab")
                    .validateHdfsConfig(errors, warnings);
            assertThat(errors).anyMatch(e -> e.contains("principal"));
            assertThat(errors).anyMatch(e -> e.contains("Keytab file not found"));

            errors.clear();
            Path keytab = Files.createFile(dir.resolve("svc.keytab"));
            new HdfsStartupRules("hdfs://nn", "/data", true, "svc@REALM", keytab.toString())
                    .validateHdfsConfig(errors, warnings);
            assertThat(errors).isEmpty();
        }
    }

    @Nested
    @DisplayName("STS")
    class Sts {

        @Test
        @DisplayName("requires an http(s) token URL, a client id and a secret")
        void requiresCredentials() {
            new StsStartupRules("", "", "").validateOAuthConfig(errors, warnings);
            assertThat(errors).anyMatch(e -> e.contains("token-url"));
            assertThat(errors).anyMatch(e -> e.contains("client-id"));
            assertThat(errors).anyMatch(e -> e.contains("client-secret"));

            errors.clear();
            new StsStartupRules("ftp://sts", "id", "secret").validateOAuthConfig(errors, warnings);
            assertThat(errors).anyMatch(e -> e.contains("http://"));

            errors.clear();
            new StsStartupRules("https://sts/token", "id", "secret").validateOAuthConfig(errors, warnings);
            assertThat(errors).isEmpty();
        }
    }
}
