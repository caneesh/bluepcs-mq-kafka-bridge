package com.hcsc.bridge.diagnostics;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

/**
 * Proves at least one broker in the bootstrap list accepts a connection. Bootstrap
 * semantics mean one reachable broker is enough to discover the cluster; unreachable
 * peers are logged so a partial outage is still visible.
 */
// Ordered so validate-only output reads in the sequence operators are used to:
// the message source first, then the destinations, then the credentials.
@Component
@Order(20)
public class KafkaConnectionCheck implements ReadinessCheck {

    private static final Logger logger = LoggerFactory.getLogger(KafkaConnectionCheck.class);

    @Value("${bridge.kafka.bootstrap-servers:}")
    private String kafkaBootstrapServers;


    @Override
    public String name() {
        return "KAFKA_CONNECTION";
    }

    @Override
    public CheckResult run() {
        String name = name();

        if (isBlank(kafkaBootstrapServers)) {
            return CheckResult.skip(name, "Kafka bootstrap servers not configured");
        }

        // Bootstrap semantics: the client only needs one reachable broker to discover the
        // cluster, so a single reachable broker passes. Unreachable brokers are logged so a
        // partial outage is still visible.
        String[] servers = kafkaBootstrapServers.split(",");
        List<String> failures = new ArrayList<>();
        for (String server : servers) {
            // Parse inside the per-server try: a malformed entry ("host:9093x") must
            // count as an unreachable broker (clean [FAIL]), not escape as a
            // NumberFormatException that turns the whole run into RESULT: EXCEPTION.
            try (java.net.Socket socket = new java.net.Socket()) {
                String[] parts = server.trim().split(":");
                String host = parts[0];
                int port = parts.length > 1 ? Integer.parseInt(parts[1]) : 9092;
                socket.connect(new java.net.InetSocketAddress(host, port), 5000);

                if (!failures.isEmpty()) {
                    logger.warn("{}: some Kafka brokers unreachable: {}", name, failures);
                }
                String message = String.format("Kafka reachable at %s:%d", host, port);
                logger.info("[PASS] {}: {}", name, message);
                return CheckResult.pass(name, message);
            } catch (Exception e) {
                failures.add(String.format("%s (%s)", server.trim(), e.getMessage()));
            }
        }

        String message = "No Kafka brokers reachable: " + failures;
        logger.error("[FAIL] {}: {}", name, message);
        return CheckResult.fail(name, message);
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }
}
