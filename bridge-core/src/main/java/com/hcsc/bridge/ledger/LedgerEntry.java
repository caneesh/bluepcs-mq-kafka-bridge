package com.hcsc.bridge.ledger;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.Objects;

public final class LedgerEntry {

    private final String messageId;
    private final String transactionId;
    private final LedgerState state;
    private final String hdfsPath;
    private final String checksum;
    private final String kafkaOffset;
    private final Instant createdAt;
    private final Instant updatedAt;
    private final int retryCount;
    private final String lastError;

    @JsonCreator
    public LedgerEntry(
            @JsonProperty("messageId") String messageId,
            @JsonProperty("transactionId") String transactionId,
            @JsonProperty("state") LedgerState state,
            @JsonProperty("hdfsPath") String hdfsPath,
            @JsonProperty("checksum") String checksum,
            @JsonProperty("kafkaOffset") String kafkaOffset,
            @JsonProperty("createdAt") Instant createdAt,
            @JsonProperty("updatedAt") Instant updatedAt,
            @JsonProperty("retryCount") int retryCount,
            @JsonProperty("lastError") String lastError) {
        this.messageId = Objects.requireNonNull(messageId, "messageId must not be null");
        this.transactionId = transactionId;
        this.state = Objects.requireNonNull(state, "state must not be null");
        this.hdfsPath = hdfsPath;
        this.checksum = checksum;
        this.kafkaOffset = kafkaOffset;
        this.createdAt = Objects.requireNonNull(createdAt, "createdAt must not be null");
        this.updatedAt = Objects.requireNonNull(updatedAt, "updatedAt must not be null");
        this.retryCount = retryCount;
        this.lastError = lastError;
    }

    private LedgerEntry(Builder builder) {
        this.messageId = Objects.requireNonNull(builder.messageId, "messageId must not be null");
        this.transactionId = builder.transactionId;
        this.state = Objects.requireNonNull(builder.state, "state must not be null");
        this.hdfsPath = builder.hdfsPath;
        this.checksum = builder.checksum;
        this.kafkaOffset = builder.kafkaOffset;
        this.createdAt = Objects.requireNonNull(builder.createdAt, "createdAt must not be null");
        this.updatedAt = Objects.requireNonNull(builder.updatedAt, "updatedAt must not be null");
        this.retryCount = builder.retryCount;
        this.lastError = builder.lastError;
    }

    public static Builder builder() {
        return new Builder();
    }

    public Builder toBuilder() {
        return new Builder()
                .messageId(this.messageId)
                .transactionId(this.transactionId)
                .state(this.state)
                .hdfsPath(this.hdfsPath)
                .checksum(this.checksum)
                .kafkaOffset(this.kafkaOffset)
                .createdAt(this.createdAt)
                .updatedAt(this.updatedAt)
                .retryCount(this.retryCount)
                .lastError(this.lastError);
    }

    public String getMessageId() {
        return messageId;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public LedgerState getState() {
        return state;
    }

    public String getHdfsPath() {
        return hdfsPath;
    }

    public String getChecksum() {
        return checksum;
    }

    public String getKafkaOffset() {
        return kafkaOffset;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }

    public int getRetryCount() {
        return retryCount;
    }

    public String getLastError() {
        return lastError;
    }

    public LedgerEntry withState(LedgerState newState) {
        return toBuilder()
                .state(newState)
                .updatedAt(Instant.now())
                .build();
    }

    public LedgerEntry withHdfsPath(String hdfsPath, String checksum) {
        return toBuilder()
                .hdfsPath(hdfsPath)
                .checksum(checksum)
                .updatedAt(Instant.now())
                .build();
    }

    public LedgerEntry withKafkaOffset(String offset) {
        return toBuilder()
                .kafkaOffset(offset)
                .updatedAt(Instant.now())
                .build();
    }

    public LedgerEntry withError(String error) {
        return toBuilder()
                .lastError(error)
                .retryCount(this.retryCount + 1)
                .updatedAt(Instant.now())
                .build();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LedgerEntry that = (LedgerEntry) o;
        return Objects.equals(messageId, that.messageId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(messageId);
    }

    @Override
    public String toString() {
        return "LedgerEntry{" +
                "messageId='" + messageId + '\'' +
                ", transactionId='" + transactionId + '\'' +
                ", state=" + state +
                ", hdfsPath='" + hdfsPath + '\'' +
                ", checksum='" + checksum + '\'' +
                ", kafkaOffset='" + kafkaOffset + '\'' +
                ", retryCount=" + retryCount +
                '}';
    }

    public static final class Builder {
        private String messageId;
        private String transactionId;
        private LedgerState state;
        private String hdfsPath;
        private String checksum;
        private String kafkaOffset;
        private Instant createdAt = Instant.now();
        private Instant updatedAt = Instant.now();
        private int retryCount = 0;
        private String lastError;

        private Builder() {}

        public Builder messageId(String messageId) {
            this.messageId = messageId;
            return this;
        }

        public Builder transactionId(String transactionId) {
            this.transactionId = transactionId;
            return this;
        }

        public Builder state(LedgerState state) {
            this.state = state;
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

        public Builder kafkaOffset(String kafkaOffset) {
            this.kafkaOffset = kafkaOffset;
            return this;
        }

        public Builder createdAt(Instant createdAt) {
            this.createdAt = createdAt;
            return this;
        }

        public Builder updatedAt(Instant updatedAt) {
            this.updatedAt = updatedAt;
            return this;
        }

        public Builder retryCount(int retryCount) {
            this.retryCount = retryCount;
            return this;
        }

        public Builder lastError(String lastError) {
            this.lastError = lastError;
            return this;
        }

        public LedgerEntry build() {
            return new LedgerEntry(this);
        }
    }
}
