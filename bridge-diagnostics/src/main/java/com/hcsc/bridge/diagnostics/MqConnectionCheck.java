package com.hcsc.bridge.diagnostics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

/**
 * Proves the queue manager accepts a TCP connection on the configured host and port.
 * Deliberately not a channel or queue authorisation check: those fail at startup in the
 * validator, and a socket probe is the part that distinguishes "wrong host or firewall"
 * from everything else.
 */
// Ordered so validate-only output reads in the sequence operators are used to:
// the message source first, then the destinations, then the credentials.
@Component
@Order(10)
public class MqConnectionCheck implements ReadinessCheck {

    private static final Logger logger = LoggerFactory.getLogger(MqConnectionCheck.class);

    @Value("${bridge.mq.host:}")
    private String mqHost;

    @Value("${bridge.mq.port:1414}")
    private int mqPort;


    @Override
    public String name() {
        return "MQ_CONNECTION";
    }

    @Override
    public CheckResult run() {
        String name = name();

        if (isBlank(mqHost)) {
            return CheckResult.skip(name, "MQ host not configured");
        }

        try {
            try (java.net.Socket socket = new java.net.Socket()) {
                socket.connect(new java.net.InetSocketAddress(mqHost, mqPort), 5000);
            }
            String message = String.format("MQ reachable at %s:%d", mqHost, mqPort);
            logger.info("[PASS] {}: {}", name, message);
            return CheckResult.pass(name, message);
        } catch (Exception e) {
            String message = String.format("Cannot reach MQ at %s:%d - %s", mqHost, mqPort, e.getMessage());
            logger.error("[FAIL] {}: {}", name, message);
            return CheckResult.fail(name, message);
        }
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }
}
