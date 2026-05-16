package com.enterprise.bridge.ledger;

public enum LedgerState {

    RECEIVED("Message received from MQ"),
    PARSED("Message payload parsed"),
    ENRICHED("Payload enriched with API data"),
    HDFS_WRITTEN("Payload written to HDFS"),
    KAFKA_PUBLISHED("Envelope published to Kafka"),
    COMPLETED("Processing completed successfully"),
    FAILED_PARSE("Failed during parsing"),
    FAILED_ENRICHMENT("Failed during enrichment"),
    FAILED_HDFS("Failed during HDFS write"),
    FAILED_KAFKA("Failed during Kafka publish"),
    RECOVERY_PENDING("Awaiting recovery"),
    DUPLICATE_DETECTED("Duplicate message detected");

    private final String description;

    LedgerState(String description) {
        this.description = description;
    }

    public String getDescription() {
        return description;
    }

    public boolean isTerminal() {
        return this == COMPLETED || this == DUPLICATE_DETECTED;
    }

    public boolean isFailed() {
        return this == FAILED_PARSE ||
                this == FAILED_ENRICHMENT ||
                this == FAILED_HDFS ||
                this == FAILED_KAFKA;
    }

    public boolean isRecoverable() {
        return this == FAILED_ENRICHMENT ||
                this == FAILED_HDFS ||
                this == FAILED_KAFKA ||
                this == RECOVERY_PENDING;
    }

    public boolean requiresHdfsRecovery() {
        return this == ENRICHED || this == FAILED_HDFS;
    }

    public boolean requiresKafkaRecovery() {
        return this == HDFS_WRITTEN || this == FAILED_KAFKA;
    }
}
