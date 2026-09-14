package com.hcsc.bridge.mock;

import com.hcsc.bridge.model.MqMessage;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

public class FakeMqMessageGenerator {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    private final AtomicLong sequenceNumber = new AtomicLong(0);
    private String defaultSourceQueue = "DEV.QUEUE.1";
    private String defaultEventType = "order_created";
    private String defaultEntityPrefix = "ENTITY";

    public MqMessage generateMessage() {
        long seq = sequenceNumber.incrementAndGet();
        String messageId = "MSG-" + UUID.randomUUID().toString().substring(0, 8);
        String correlationId = "CORR-" + seq;
        String payload = generatePayload(messageId, seq);

        return new MqMessage(
                messageId,
                correlationId,
                payload,
                Instant.now(),
                defaultSourceQueue
        );
    }

    public MqMessage generateMessageWithId(String messageId) {
        long seq = sequenceNumber.incrementAndGet();
        String correlationId = "CORR-" + seq;
        String payload = generatePayload(messageId, seq);

        return new MqMessage(
                messageId,
                correlationId,
                payload,
                Instant.now(),
                defaultSourceQueue
        );
    }

    public MqMessage generateMessageWithPayload(String messageId, String rawPayload) {
        long seq = sequenceNumber.incrementAndGet();
        return new MqMessage(
                messageId,
                "CORR-" + seq,
                rawPayload,
                Instant.now(),
                defaultSourceQueue
        );
    }

    private String generatePayload(String messageId, long seq) {
        String marketingPlanIdentifier = defaultEntityPrefix + "-" + seq;

        Map<String, Object> effectivityDates = new HashMap<>();
        effectivityDates.put("effectiveStartDate", "2026-01-01T06:00:00.000Z");
        effectivityDates.put("retiredDate", "9999-12-31T00:00:00.000Z");
        effectivityDates.put("closedDate", "9999-12-31T00:00:00.000Z");

        Map<String, Object> planVersion = new HashMap<>();
        planVersion.put("planVersionIdentifier", String.valueOf(seq));
        planVersion.put("planEffectivityDates", effectivityDates);
        planVersion.put("planStatus", "Open");

        Map<String, Object> changeEvent = new HashMap<>();
        changeEvent.put("typeName", "Update");
        changeEvent.put("eventName", defaultEventType);
        changeEvent.put("timestamp", Instant.now().toString());

        Map<String, Object> planNotification = new HashMap<>();
        planNotification.put("marketingPlanIdentifier", marketingPlanIdentifier);
        planNotification.put("lineOfBusiness", "Retail");
        planNotification.put("planVersion", planVersion);
        planNotification.put("changeEvent", changeEvent);
        planNotification.put("mockIndicator", "N");

        Map<String, Object> payload = new HashMap<>();
        payload.put("planNotification", planNotification);

        try {
            return OBJECT_MAPPER.writeValueAsString(payload);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to generate payload", e);
        }
    }

    public MqMessage generateInvalidMessage(String messageId) {
        return new MqMessage(
                messageId,
                "CORR-INVALID",
                "{ invalid json }",
                Instant.now(),
                defaultSourceQueue
        );
    }

    public MqMessage generateMessageWithMissingFields(String messageId) {
        Map<String, Object> payload = new HashMap<>();
        payload.put("messageId", messageId);

        try {
            return new MqMessage(
                    messageId,
                    "CORR-INCOMPLETE",
                    OBJECT_MAPPER.writeValueAsString(payload),
                    Instant.now(),
                    defaultSourceQueue
            );
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to generate payload", e);
        }
    }

    public void setDefaultSourceQueue(String queue) {
        this.defaultSourceQueue = queue;
    }

    public void setDefaultEventType(String eventType) {
        this.defaultEventType = eventType;
    }

    public void setDefaultEntityPrefix(String prefix) {
        this.defaultEntityPrefix = prefix;
    }

    public void reset() {
        sequenceNumber.set(0);
        defaultSourceQueue = "DEV.QUEUE.1";
        defaultEventType = "order_created";
        defaultEntityPrefix = "ENTITY";
    }
}
