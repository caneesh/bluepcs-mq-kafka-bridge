package com.hcsc.bridge.model;

import java.time.Instant;
import java.util.Objects;

public final class MqMessage {

    private final String messageId;
    private final String correlationId;
    private final String payload;
    private final Instant receivedAt;
    private final String sourceQueue;

    public MqMessage(String messageId, String correlationId, String payload,
                     Instant receivedAt, String sourceQueue) {
        this.messageId = Objects.requireNonNull(messageId, "messageId must not be null");
        this.correlationId = correlationId;
        this.payload = Objects.requireNonNull(payload, "payload must not be null");
        this.receivedAt = Objects.requireNonNull(receivedAt, "receivedAt must not be null");
        this.sourceQueue = sourceQueue;
    }

    public String getMessageId() {
        return messageId;
    }

    public String getCorrelationId() {
        return correlationId;
    }

    public String getPayload() {
        return payload;
    }

    public Instant getReceivedAt() {
        return receivedAt;
    }

    public String getSourceQueue() {
        return sourceQueue;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MqMessage mqMessage = (MqMessage) o;
        return Objects.equals(messageId, mqMessage.messageId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(messageId);
    }

    @Override
    public String toString() {
        return "MqMessage{" +
                "messageId='" + messageId + '\'' +
                ", correlationId='" + correlationId + '\'' +
                ", receivedAt=" + receivedAt +
                ", sourceQueue='" + sourceQueue + '\'' +
                '}';
    }
}
