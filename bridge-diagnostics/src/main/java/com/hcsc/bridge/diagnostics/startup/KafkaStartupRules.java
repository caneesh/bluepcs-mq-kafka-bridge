package com.hcsc.bridge.diagnostics.startup;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;

/**
 * The Kafka transport rules both bridges enforce when they use Kafka at all - the PMM+
 * bridge for claim-check notifications, the PMM bridge for the audit stream. The topic an
 * application publishes to, and the coherence between a publisher's wait and the producer's
 * own budget, stay with the application that does the publishing.
 */
public final class KafkaStartupRules {

    private static final Logger logger = LoggerFactory.getLogger(KafkaStartupRules.class);

    private final String kafkaBootstrapServers;
    private final String kafkaSecurityProtocol;
    private final String kafkaTruststoreLocation;
    private final String kafkaTruststorePassword;
    private final String kafkaAcks;

    public KafkaStartupRules(String kafkaBootstrapServers, String kafkaSecurityProtocol, String kafkaTruststoreLocation, String kafkaTruststorePassword, String kafkaAcks) {
        this.kafkaBootstrapServers = kafkaBootstrapServers;
        this.kafkaSecurityProtocol = kafkaSecurityProtocol;
        this.kafkaTruststoreLocation = kafkaTruststoreLocation;
        this.kafkaTruststorePassword = kafkaTruststorePassword;
        this.kafkaAcks = kafkaAcks;
    }

    public void validateKafkaTransport(List<String> errors, List<String> warnings) {
        logger.info("Validating Kafka configuration...");

        if (isBlank(kafkaBootstrapServers)) {
            errors.add("[KAFKA] bridge.kafka.bootstrap-servers is required");
        } else {
            logger.info("[KAFKA] Bootstrap Servers: {}", kafkaBootstrapServers);
        }


        logger.info("[KAFKA] Security Protocol: {}", kafkaSecurityProtocol);

        // KafkaConfiguration hard-enables idempotence, which the producer only accepts
        // with acks=all. Any other value fails at FIRST SEND (lazy producer
        // construction), not startup — surface it here instead.
        if (!"all".equalsIgnoreCase(kafkaAcks) && !"-1".equals(kafkaAcks)) {
            errors.add("[KAFKA] bridge.kafka.acks=" + kafkaAcks + " is invalid: the producer "
                    + "runs with enable.idempotence=true, which requires acks=all");
        }

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
    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }
}
