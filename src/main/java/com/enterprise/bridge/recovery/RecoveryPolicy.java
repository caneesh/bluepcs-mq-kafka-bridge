package com.enterprise.bridge.recovery;

import com.enterprise.bridge.ledger.LedgerEntry;
import com.enterprise.bridge.ledger.LedgerState;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class RecoveryPolicy {

    private final int maxRetries;

    public RecoveryPolicy(@Value("${bridge.recovery.max-retries:3}") int maxRetries) {
        this.maxRetries = maxRetries;
    }

    public RecoveryAction determineAction(LedgerEntry entry) {
        if (entry == null) {
            return RecoveryAction.NO_ACTION;
        }

        LedgerState state = entry.getState();
        int retryCount = entry.getRetryCount();

        if (retryCount >= maxRetries) {
            return RecoveryAction.MANUAL_INTERVENTION;
        }

        switch (state) {
            case RECEIVED:
            case PARSED:
                return RecoveryAction.SKIP;

            case ENRICHED:
            case FAILED_HDFS:
                return RecoveryAction.RETRY_HDFS_WRITE;

            case HDFS_WRITTEN:
            case FAILED_KAFKA:
                return RecoveryAction.RETRY_KAFKA_PUBLISH;

            case FAILED_ENRICHMENT:
                return RecoveryAction.RETRY_ENRICHMENT;

            case RECOVERY_PENDING:
                return determineRecoveryPendingAction(entry);

            case COMPLETED:
            case DUPLICATE_DETECTED:
                return RecoveryAction.NO_ACTION;

            case FAILED_PARSE:
                return RecoveryAction.MANUAL_INTERVENTION;

            default:
                return RecoveryAction.MANUAL_INTERVENTION;
        }
    }

    private RecoveryAction determineRecoveryPendingAction(LedgerEntry entry) {
        if (entry.getHdfsPath() != null && entry.getKafkaOffset() == null) {
            return RecoveryAction.RETRY_KAFKA_PUBLISH;
        }
        if (entry.getHdfsPath() == null && entry.getTransactionId() != null) {
            return RecoveryAction.RETRY_HDFS_WRITE;
        }
        if (entry.getTransactionId() == null) {
            return RecoveryAction.SKIP;
        }
        return RecoveryAction.MANUAL_INTERVENTION;
    }

    public boolean shouldRetry(LedgerEntry entry) {
        RecoveryAction action = determineAction(entry);
        return action != RecoveryAction.NO_ACTION &&
                action != RecoveryAction.MANUAL_INTERVENTION &&
                action != RecoveryAction.SKIP;
    }

    public int getMaxRetries() {
        return maxRetries;
    }
}
