package com.enterprise.bridge.audit;

import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

public final class AuditEvent {

    private final String eventId;
    private final String messageId;
    private final String transactionId;
    private final AuditEventType eventType;
    private final String description;
    private final Map<String, Object> metadata;
    private final Instant timestamp;
    private final String errorMessage;

    private AuditEvent(Builder builder) {
        this.eventId = Objects.requireNonNull(builder.eventId, "eventId must not be null");
        this.messageId = builder.messageId;
        this.transactionId = builder.transactionId;
        this.eventType = Objects.requireNonNull(builder.eventType, "eventType must not be null");
        this.description = builder.description;
        this.metadata = builder.metadata != null ? Collections.unmodifiableMap(builder.metadata) : Collections.emptyMap();
        this.timestamp = Objects.requireNonNull(builder.timestamp, "timestamp must not be null");
        this.errorMessage = builder.errorMessage;
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getEventId() {
        return eventId;
    }

    public String getMessageId() {
        return messageId;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public AuditEventType getEventType() {
        return eventType;
    }

    public String getDescription() {
        return description;
    }

    public Map<String, Object> getMetadata() {
        return metadata;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    @Override
    public String toString() {
        return "AuditEvent{" +
                "eventId='" + eventId + '\'' +
                ", messageId='" + messageId + '\'' +
                ", eventType=" + eventType +
                ", timestamp=" + timestamp +
                '}';
    }

    public static final class Builder {
        private String eventId;
        private String messageId;
        private String transactionId;
        private AuditEventType eventType;
        private String description;
        private Map<String, Object> metadata;
        private Instant timestamp = Instant.now();
        private String errorMessage;

        private Builder() {}

        public Builder eventId(String eventId) {
            this.eventId = eventId;
            return this;
        }

        public Builder messageId(String messageId) {
            this.messageId = messageId;
            return this;
        }

        public Builder transactionId(String transactionId) {
            this.transactionId = transactionId;
            return this;
        }

        public Builder eventType(AuditEventType eventType) {
            this.eventType = eventType;
            return this;
        }

        public Builder description(String description) {
            this.description = description;
            return this;
        }

        public Builder metadata(Map<String, Object> metadata) {
            this.metadata = metadata;
            return this;
        }

        public Builder timestamp(Instant timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public Builder errorMessage(String errorMessage) {
            this.errorMessage = errorMessage;
            return this;
        }

        public AuditEvent build() {
            return new AuditEvent(this);
        }
    }
}
