package com.enterprise.bridge.recovery;

import com.enterprise.bridge.audit.AuditEvent;
import com.enterprise.bridge.audit.AuditEventType;
import com.enterprise.bridge.audit.AuditPublisher;
import com.enterprise.bridge.ledger.LedgerEntry;
import com.enterprise.bridge.ledger.LedgerRepository;
import com.enterprise.bridge.ledger.LedgerState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Service
@Profile("local")
public class RecoveryService {

    private static final Logger logger = LoggerFactory.getLogger(RecoveryService.class);

    private final LedgerRepository ledgerRepository;
    private final RecoveryPolicy recoveryPolicy;
    private final AuditPublisher auditPublisher;

    public RecoveryService(LedgerRepository ledgerRepository,
                           RecoveryPolicy recoveryPolicy,
                           AuditPublisher auditPublisher) {
        this.ledgerRepository = ledgerRepository;
        this.recoveryPolicy = recoveryPolicy;
        this.auditPublisher = auditPublisher;
    }

    @Scheduled(fixedDelayString = "${bridge.recovery.interval-ms:60000}")
    public void processRecovery() {
        List<LedgerEntry> recoverableEntries = ledgerRepository.findRecoverableEntries();

        if (recoverableEntries.isEmpty()) {
            logger.debug("No recoverable entries found");
            return;
        }

        logger.info("Found {} recoverable entries", recoverableEntries.size());

        for (LedgerEntry entry : recoverableEntries) {
            try {
                RecoveryAction action = recoveryPolicy.determineAction(entry);
                logger.info("Recovery action for {}: {}", entry.getMessageId(), action);

                publishAudit(entry.getMessageId(), entry.getTransactionId(),
                        AuditEventType.RECOVERY_STARTED, "Recovery action: " + action);

                LedgerEntry updated = entry.withState(LedgerState.RECOVERY_PENDING);
                ledgerRepository.update(updated);

            } catch (Exception e) {
                logger.error("Recovery failed for entry: {}", entry.getMessageId(), e);
                publishAudit(entry.getMessageId(), entry.getTransactionId(),
                        AuditEventType.RECOVERY_FAILED, "Recovery failed: " + e.getMessage());
            }
        }
    }

    private void publishAudit(String messageId, String transactionId,
                              AuditEventType eventType, String description) {
        AuditEvent event = AuditEvent.builder()
                .auditEventId(UUID.randomUUID().toString())
                .messageId(messageId)
                .transactionId(transactionId)
                .eventType(eventType)
                .description(description)
                .timestamp(Instant.now())
                .build();
        auditPublisher.publishAsync(event);
    }
}
