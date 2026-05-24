package com.hcsc.bridge.audit;

import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

public final class AuditEvent {

    private final String auditEventId;
    private final String eventId;
    private final String bridgeEventId;
    private final String originalMqMessageId;
    private final String messageId;
    private final String transactionId;
    private final AuditEventType eventType;
    private final String description;
    private final Map<String, Object> metadata;
    private final Instant timestamp;
    private final String errorMessage;

    private AuditEvent(Builder builder) {
        this.auditEventId = Objects.requireNonNull(builder.auditEventId, "auditEventId must not be null");
        this.eventId = builder.eventId;
        this.bridgeEventId = builder.bridgeEventId;
        this.originalMqMessageId = builder.originalMqMessageId;
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

    public String getAuditEventId() {
        return auditEventId;
    }

    public String getEventId() {
        return eventId;
    }

    public String getBridgeEventId() {
        return bridgeEventId;
    }

    public String getOriginalMqMessageId() {
        return originalMqMessageId;
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
                "auditEventId='" + auditEventId + '\'' +
                ", eventId='" + eventId + '\'' +
                ", bridgeEventId='" + bridgeEventId + '\'' +
                ", eventType=" + eventType +
                ", timestamp=" + timestamp +
                '}';
    }

    public static final class Builder {
        private String auditEventId;
        private String eventId;
        private String bridgeEventId;
        private String originalMqMessageId;
        private String messageId;
        private String transactionId;
        private AuditEventType eventType;
        private String description;
        private Map<String, Object> metadata;
        private Instant timestamp = Instant.now();
        private String errorMessage;

        private Builder() {}

        public Builder auditEventId(String auditEventId) {
            this.auditEventId = auditEventId;
            return this;
        }

        public Builder eventId(String eventId) {
            this.eventId = eventId;
            return this;
        }

        public Builder bridgeEventId(String bridgeEventId) {
            this.bridgeEventId = bridgeEventId;
            return this;
        }

        public Builder originalMqMessageId(String originalMqMessageId) {
            this.originalMqMessageId = originalMqMessageId;
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
