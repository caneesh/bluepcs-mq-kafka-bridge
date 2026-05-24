package com.hcsc.bridge.reconciliation;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public final class ReconciliationResult {

    private final Instant timestamp;
    private final long ledgerCount;
    private final long hdfsCount;
    private final long kafkaCount;
    private final boolean balanced;
    private final List<ReconciliationFinding> findings;

    public ReconciliationResult(Instant timestamp, long ledgerCount, long hdfsCount,
                                long kafkaCount, boolean balanced,
                                List<ReconciliationFinding> findings) {
        this.timestamp = Objects.requireNonNull(timestamp, "timestamp must not be null");
        this.ledgerCount = ledgerCount;
        this.hdfsCount = hdfsCount;
        this.kafkaCount = kafkaCount;
        this.balanced = balanced;
        this.findings = findings != null ? Collections.unmodifiableList(findings) : Collections.emptyList();
    }

    public static ReconciliationResult balanced(long count) {
        return new ReconciliationResult(Instant.now(), count, count, count, true, Collections.emptyList());
    }

    public static ReconciliationResult unbalanced(long ledgerCount, long hdfsCount, long kafkaCount,
                                                  List<ReconciliationFinding> findings) {
        return new ReconciliationResult(Instant.now(), ledgerCount, hdfsCount, kafkaCount, false, findings);
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public long getLedgerCount() {
        return ledgerCount;
    }

    public long getHdfsCount() {
        return hdfsCount;
    }

    public long getKafkaCount() {
        return kafkaCount;
    }

    public boolean isBalanced() {
        return balanced;
    }

    public List<ReconciliationFinding> getFindings() {
        return findings;
    }

    @Override
    public String toString() {
        return "ReconciliationResult{" +
                "timestamp=" + timestamp +
                ", ledgerCount=" + ledgerCount +
                ", hdfsCount=" + hdfsCount +
                ", kafkaCount=" + kafkaCount +
                ", balanced=" + balanced +
                ", findings=" + findings.size() +
                '}';
    }
}
