package com.hcsc.bridge.reconciliation;

import com.hcsc.bridge.ledger.LedgerEntry;
import com.hcsc.bridge.ledger.LedgerRepository;
import com.hcsc.bridge.ledger.LedgerState;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
@ConditionalOnProperty(name = "bridge.reconciliation.enabled", havingValue = "true", matchIfMissing = false)
public class ReconciliationPolicy {

    private final LedgerRepository ledgerRepository;

    public ReconciliationPolicy(LedgerRepository ledgerRepository) {
        this.ledgerRepository = ledgerRepository;
    }

    public ReconciliationResult reconcile(long hdfsCount, long kafkaCount) {
        long completedCount = ledgerRepository.countByState(LedgerState.COMPLETED);
        long hdfsWrittenCount = ledgerRepository.countByState(LedgerState.HDFS_WRITTEN);
        long kafkaPublishedCount = ledgerRepository.countByState(LedgerState.KAFKA_PUBLISHED);

        long expectedHdfsCount = completedCount + hdfsWrittenCount + kafkaPublishedCount;
        long expectedKafkaCount = completedCount + kafkaPublishedCount;

        if (hdfsCount == expectedHdfsCount && kafkaCount == expectedKafkaCount) {
            return ReconciliationResult.balanced(completedCount);
        }

        List<ReconciliationFinding> findings = generateFindings(hdfsCount, kafkaCount,
                expectedHdfsCount, expectedKafkaCount);

        return ReconciliationResult.unbalanced(completedCount, hdfsCount, kafkaCount, findings);
    }

    private List<ReconciliationFinding> generateFindings(long actualHdfs, long actualKafka,
                                                         long expectedHdfs, long expectedKafka) {
        List<ReconciliationFinding> findings = new ArrayList<>();

        if (actualHdfs < expectedHdfs) {
            findings.add(new ReconciliationFinding(
                    "SYSTEM",
                    ReconciliationFinding.FindingType.MISSING_IN_HDFS,
                    "Missing " + (expectedHdfs - actualHdfs) + " files in HDFS"
            ));
        } else if (actualHdfs > expectedHdfs) {
            findings.add(new ReconciliationFinding(
                    "SYSTEM",
                    ReconciliationFinding.FindingType.ORPHAN_HDFS_FILE,
                    "Found " + (actualHdfs - expectedHdfs) + " orphan files in HDFS"
            ));
        }

        if (actualKafka < expectedKafka) {
            findings.add(new ReconciliationFinding(
                    "SYSTEM",
                    ReconciliationFinding.FindingType.MISSING_IN_KAFKA,
                    "Missing " + (expectedKafka - actualKafka) + " messages in Kafka"
            ));
        } else if (actualKafka > expectedKafka) {
            findings.add(new ReconciliationFinding(
                    "SYSTEM",
                    ReconciliationFinding.FindingType.ORPHAN_KAFKA_MESSAGE,
                    "Found " + (actualKafka - expectedKafka) + " orphan messages in Kafka"
            ));
        }

        List<LedgerEntry> staleEntries = ledgerRepository.findByStates(List.of(
                LedgerState.ENRICHED, LedgerState.HDFS_WRITTEN));
        for (LedgerEntry entry : staleEntries) {
            findings.add(new ReconciliationFinding(
                    entry.getMessageId(),
                    ReconciliationFinding.FindingType.LEDGER_STALE,
                    "Entry stuck in state: " + entry.getState()
            ));
        }

        return findings;
    }
}
