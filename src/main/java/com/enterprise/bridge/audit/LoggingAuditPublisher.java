package com.enterprise.bridge.audit;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

@Component
public class LoggingAuditPublisher implements AuditPublisher {

    private static final Logger auditLogger = LoggerFactory.getLogger("audit");
    private final ObjectMapper objectMapper;

    public LoggingAuditPublisher() {
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new JavaTimeModule());
    }

    @Override
    public void publish(AuditEvent event) {
        try {
            String json = objectMapper.writeValueAsString(event);
            auditLogger.info("{}", json);
        } catch (Exception e) {
            auditLogger.error("Failed to serialize audit event: {}", event, e);
        }
    }

    @Override
    @Async
    public void publishAsync(AuditEvent event) {
        publish(event);
    }
}
