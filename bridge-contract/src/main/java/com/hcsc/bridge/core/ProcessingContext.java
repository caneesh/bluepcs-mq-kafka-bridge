package com.hcsc.bridge.core;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

public final class ProcessingContext {

    private final String eventId;
    private final String bridgeMessageId;
    private final String originalMqMessageId;
    private final Instant receivedAt;

    /**
     * originalMqMessageId is nullable: JMS permits a null JMSMessageID, and the
     * orchestrator derives the (required) eventId from the payload in that case.
     */
    public ProcessingContext(String eventId, String originalMqMessageId, Instant receivedAt) {
        this.eventId = Objects.requireNonNull(eventId, "eventId must not be null");
        this.bridgeMessageId = UUID.randomUUID().toString();
        this.originalMqMessageId = originalMqMessageId;
        this.receivedAt = Objects.requireNonNull(receivedAt, "receivedAt must not be null");
    }

    public String getEventId() {
        return eventId;
    }

    public String getBridgeMessageId() {
        return bridgeMessageId;
    }

    public String getOriginalMqMessageId() {
        return originalMqMessageId;
    }

    public Instant getReceivedAt() {
        return receivedAt;
    }

    @Override
    public String toString() {
        return "ProcessingContext{" +
                "eventId='" + eventId + '\'' +
                ", bridgeMessageId='" + bridgeMessageId + '\'' +
                ", originalMqMessageId='" + originalMqMessageId + '\'' +
                '}';
    }
}
