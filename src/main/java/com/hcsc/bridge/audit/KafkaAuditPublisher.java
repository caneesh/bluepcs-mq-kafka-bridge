package com.hcsc.bridge.audit;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Component
@Profile("!local")
public class KafkaAuditPublisher implements AuditPublisher {

    private static final Logger logger = LoggerFactory.getLogger(KafkaAuditPublisher.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final String auditTopic;
    private final ObjectMapper objectMapper;
    private final int timeoutSeconds;

    public KafkaAuditPublisher(
            KafkaTemplate<String, String> kafkaTemplate,
            @Value("${bridge.kafka.audit-topic:bridge-audit}") String auditTopic,
            @Value("${bridge.kafka.audit-timeout-seconds:5}") int timeoutSeconds) {
        this.kafkaTemplate = kafkaTemplate;
        this.auditTopic = auditTopic;
        this.timeoutSeconds = timeoutSeconds;
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new JavaTimeModule());
    }

    @Override
    public void publish(AuditEvent event) {
        try {
            String key = event.getEventId() != null ? event.getEventId() : event.getAuditEventId();
            String json = objectMapper.writeValueAsString(event);

            kafkaTemplate.send(auditTopic, key, json)
                    .get(timeoutSeconds, TimeUnit.SECONDS);

            logger.debug("Published audit event: {} for eventId: {}", event.getEventType(), event.getEventId());
        } catch (JsonProcessingException e) {
            logger.error("Failed to serialize audit event: {}", event, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Interrupted publishing audit event: {}", event.getAuditEventId());
        } catch (ExecutionException | TimeoutException e) {
            logger.error("Failed to publish audit event: {}", event.getAuditEventId(), e);
        }
    }

    @Override
    public void publishAsync(AuditEvent event) {
        try {
            String key = event.getEventId() != null ? event.getEventId() : event.getAuditEventId();
            String json = objectMapper.writeValueAsString(event);

            kafkaTemplate.send(auditTopic, key, json)
                    .addCallback(
                            result -> logger.debug("Async audit published: {}", event.getAuditEventId()),
                            ex -> logger.error("Async audit failed: {}", event.getAuditEventId(), ex)
                    );
        } catch (JsonProcessingException e) {
            logger.error("Failed to serialize async audit event: {}", event, e);
        }
    }
}
