package com.enterprise.bridge.reconciliation;

import java.util.Objects;

public final class ReconciliationFinding {

    private final String messageId;
    private final FindingType type;
    private final String description;

    public ReconciliationFinding(String messageId, FindingType type, String description) {
        this.messageId = Objects.requireNonNull(messageId, "messageId must not be null");
        this.type = Objects.requireNonNull(type, "type must not be null");
        this.description = description;
    }

    public String getMessageId() {
        return messageId;
    }

    public FindingType getType() {
        return type;
    }

    public String getDescription() {
        return description;
    }

    @Override
    public String toString() {
        return "ReconciliationFinding{" +
                "messageId='" + messageId + '\'' +
                ", type=" + type +
                ", description='" + description + '\'' +
                '}';
    }

    public enum FindingType {
        MISSING_IN_HDFS,
        MISSING_IN_KAFKA,
        CHECKSUM_MISMATCH,
        ORPHAN_HDFS_FILE,
        ORPHAN_KAFKA_MESSAGE,
        LEDGER_STALE
    }
}
