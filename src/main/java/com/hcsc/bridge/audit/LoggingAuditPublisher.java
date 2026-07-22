package com.hcsc.bridge.audit;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Writes audit events as JSON lines to the "audit" logger. Sole publisher in the local
 * profile; in other profiles it is available as the {@code bridge.audit.publisher=log}
 * fallback delegate of {@link SafeAuditPublisher} (e.g. while the Kafka audit topic does
 * not exist yet).
 */
@Component
public class LoggingAuditPublisher implements AuditPublisher {

    private static final Logger auditLogger = LoggerFactory.getLogger("audit");
    private final ObjectMapper objectMapper;

    public LoggingAuditPublisher() {
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new JavaTimeModule());
        // Same wire contract as KafkaAuditPublisher: timestamp as ISO-8601 STRING, so the
        // log fallback's lines stay format-identical to what lands on the audit topic.
        this.objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
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
