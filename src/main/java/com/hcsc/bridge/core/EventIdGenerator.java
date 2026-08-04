package com.hcsc.bridge.core;

import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

@Component
public class EventIdGenerator {

    public String generateEventId(String jmsMessageId) {
        if (jmsMessageId == null || jmsMessageId.isEmpty()) {
            throw new IllegalArgumentException("JMS Message ID cannot be null or empty");
        }
        return DigestUtil.sha256Hex(jmsMessageId.getBytes(StandardCharsets.UTF_8));
    }
}
