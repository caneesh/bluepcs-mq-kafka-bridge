package com.enterprise.bridge.core;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

public final class ProcessingContext {

    private final String eventId;
    private final String bridgeMessageId;
    private final String originalMqMessageId;
    private final Instant receivedAt;

    public ProcessingContext(String eventId, String originalMqMessageId, Instant receivedAt) {
        this.eventId = Objects.requireNonNull(eventId, "eventId must not be null");
        this.bridgeMessageId = UUID.randomUUID().toString();
        this.originalMqMessageId = Objects.requireNonNull(originalMqMessageId, "originalMqMessageId must not be null");
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
