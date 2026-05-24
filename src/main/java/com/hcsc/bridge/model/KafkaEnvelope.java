package com.hcsc.bridge.model;

import java.time.Instant;
import java.util.Objects;

public final class KafkaEnvelope {

    private final String eventId;
    private final String bridgeMessageId;
    private final String originalMqMessageId;
    private final String messageId;
    private final String transactionId;
    private final String eventType;
    private final String entityId;
    private final String hdfsPath;
    private final String checksum;
    private final String marketingPlanId;
    private final String campaignId;
    private final Instant eventTimestamp;
    private final Instant processedAt;
    private final String schemaVersion;

    private KafkaEnvelope(Builder builder) {
        this.eventId = Objects.requireNonNull(builder.eventId, "eventId must not be null");
        this.bridgeMessageId = Objects.requireNonNull(builder.bridgeMessageId, "bridgeMessageId must not be null");
        this.originalMqMessageId = Objects.requireNonNull(builder.originalMqMessageId, "originalMqMessageId must not be null");
        this.messageId = Objects.requireNonNull(builder.messageId, "messageId must not be null");
        this.transactionId = Objects.requireNonNull(builder.transactionId, "transactionId must not be null");
        this.eventType = Objects.requireNonNull(builder.eventType, "eventType must not be null");
        this.entityId = Objects.requireNonNull(builder.entityId, "entityId must not be null");
        this.hdfsPath = Objects.requireNonNull(builder.hdfsPath, "hdfsPath must not be null");
        this.checksum = Objects.requireNonNull(builder.checksum, "checksum must not be null");
        this.marketingPlanId = builder.marketingPlanId;
        this.campaignId = builder.campaignId;
        this.eventTimestamp = Objects.requireNonNull(builder.eventTimestamp, "eventTimestamp must not be null");
        this.processedAt = Objects.requireNonNull(builder.processedAt, "processedAt must not be null");
        this.schemaVersion = Objects.requireNonNull(builder.schemaVersion, "schemaVersion must not be null");
    }

    public static Builder builder() {
        return new Builder();
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

    public String getHdfsPath() {
        return hdfsPath;
    }

    public String getChecksum() {
        return checksum;
    }

    public String getMarketingPlanId() {
        return marketingPlanId;
    }

    public String getCampaignId() {
        return campaignId;
    }

    public Instant getEventTimestamp() {
        return eventTimestamp;
    }

    public Instant getProcessedAt() {
        return processedAt;
    }

    public String getSchemaVersion() {
        return schemaVersion;
    }

    public String getKafkaKey() {
        return eventId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KafkaEnvelope that = (KafkaEnvelope) o;
        return Objects.equals(eventId, that.eventId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventId);
    }

    @Override
    public String toString() {
        return "KafkaEnvelope{" +
                "eventId='" + eventId + '\'' +
                ", bridgeMessageId='" + bridgeMessageId + '\'' +
                ", originalMqMessageId='" + originalMqMessageId + '\'' +
                ", transactionId='" + transactionId + '\'' +
                ", eventType='" + eventType + '\'' +
                ", entityId='" + entityId + '\'' +
                ", hdfsPath='" + hdfsPath + '\'' +
                '}';
    }

    public static final class Builder {
        private String eventId;
        private String bridgeMessageId;
        private String originalMqMessageId;
        private String messageId;
        private String transactionId;
        private String eventType;
        private String entityId;
        private String hdfsPath;
        private String checksum;
        private String marketingPlanId;
        private String campaignId;
        private Instant eventTimestamp;
        private Instant processedAt;
        private String schemaVersion = "1.0";

        private Builder() {}

        public Builder eventId(String eventId) {
            this.eventId = eventId;
            return this;
        }

        public Builder bridgeMessageId(String bridgeMessageId) {
            this.bridgeMessageId = bridgeMessageId;
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

        public Builder eventType(String eventType) {
            this.eventType = eventType;
            return this;
        }

        public Builder entityId(String entityId) {
            this.entityId = entityId;
            return this;
        }

        public Builder hdfsPath(String hdfsPath) {
            this.hdfsPath = hdfsPath;
            return this;
        }

        public Builder checksum(String checksum) {
            this.checksum = checksum;
            return this;
        }

        public Builder marketingPlanId(String marketingPlanId) {
            this.marketingPlanId = marketingPlanId;
            return this;
        }

        public Builder campaignId(String campaignId) {
            this.campaignId = campaignId;
            return this;
        }

        public Builder eventTimestamp(Instant eventTimestamp) {
            this.eventTimestamp = eventTimestamp;
            return this;
        }

        public Builder processedAt(Instant processedAt) {
            this.processedAt = processedAt;
            return this;
        }

        public Builder schemaVersion(String schemaVersion) {
            this.schemaVersion = schemaVersion;
            return this;
        }

        public KafkaEnvelope build() {
            return new KafkaEnvelope(this);
        }
    }
}
