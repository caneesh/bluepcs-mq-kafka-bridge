package com.enterprise.bridge.model;

import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

public final class ParsedPayload {

    private final String messageId;
    private final String transactionId;
    private final String eventType;
    private final String entityId;
    private final Map<String, Object> data;
    private final Instant eventTimestamp;
    private final String rawPayload;

    public ParsedPayload(String messageId, String transactionId, String eventType,
                         String entityId, Map<String, Object> data, Instant eventTimestamp,
                         String rawPayload) {
        this.messageId = Objects.requireNonNull(messageId, "messageId must not be null");
        this.transactionId = Objects.requireNonNull(transactionId, "transactionId must not be null");
        this.eventType = Objects.requireNonNull(eventType, "eventType must not be null");
        this.entityId = Objects.requireNonNull(entityId, "entityId must not be null");
        this.data = data != null ? Collections.unmodifiableMap(data) : Collections.emptyMap();
        this.eventTimestamp = Objects.requireNonNull(eventTimestamp, "eventTimestamp must not be null");
        this.rawPayload = Objects.requireNonNull(rawPayload, "rawPayload must not be null");
    }

    public String getMessageId() {
        return messageId;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public String getEventType() {
        return eventType;
    }

    public String getEntityId() {
        return entityId;
    }

    public Map<String, Object> getData() {
        return data;
    }

    public Instant getEventTimestamp() {
        return eventTimestamp;
    }

    public String getRawPayload() {
        return rawPayload;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ParsedPayload that = (ParsedPayload) o;
        return Objects.equals(messageId, that.messageId) &&
                Objects.equals(transactionId, that.transactionId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(messageId, transactionId);
    }

    @Override
    public String toString() {
        return "ParsedPayload{" +
                "messageId='" + messageId + '\'' +
                ", transactionId='" + transactionId + '\'' +
                ", eventType='" + eventType + '\'' +
                ", entityId='" + entityId + '\'' +
                ", eventTimestamp=" + eventTimestamp +
                '}';
    }
}
