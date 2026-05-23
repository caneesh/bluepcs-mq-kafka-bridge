package com.enterprise.bridge.integration;

import com.enterprise.bridge.ledger.HBaseLedgerRepository;
import com.enterprise.bridge.ledger.LedgerEntry;
import com.enterprise.bridge.ledger.LedgerState;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

@Disabled("Integration test requires running HBase cluster - enable when HBase is available")
@DisplayName("HBase Ledger Repository Integration Tests")
class HBaseLedgerRepositoryIT {

    private HBaseLedgerRepository repository;

    @Test
    @DisplayName("should save and retrieve entry from HBase")
    void shouldSaveAndRetrieve() {
        LedgerEntry entry = LedgerEntry.builder()
                .messageId("IT-MSG-001")
                .transactionId("IT-TXN-001")
                .state(LedgerState.RECEIVED)
                .createdAt(Instant.now())
                .updatedAt(Instant.now())
                .build();

        repository.save(entry);

        assertThat(repository.findByMessageId("IT-MSG-001"))
                .isPresent()
                .hasValueSatisfying(found -> {
                    assertThat(found.getMessageId()).isEqualTo("IT-MSG-001");
                    assertThat(found.getState()).isEqualTo(LedgerState.RECEIVED);
                });
    }

    @Test
    @DisplayName("should update entry state")
    void shouldUpdateState() {
        LedgerEntry entry = LedgerEntry.builder()
                .messageId("IT-MSG-002")
                .transactionId("IT-TXN-002")
                .state(LedgerState.RECEIVED)
                .createdAt(Instant.now())
                .updatedAt(Instant.now())
                .build();

        repository.save(entry);

        LedgerEntry updated = entry.withState(LedgerState.KAFKA_PUBLISHED);
        repository.update(updated);

        assertThat(repository.findByMessageId("IT-MSG-002"))
                .isPresent()
                .hasValueSatisfying(found ->
                    assertThat(found.getState()).isEqualTo(LedgerState.KAFKA_PUBLISHED)
                );
    }

    @Test
    @DisplayName("should find entries by state")
    void shouldFindByState() {
        repository.save(LedgerEntry.builder()
                .messageId("IT-MSG-003")
                .state(LedgerState.FAILED_KAFKA)
                .createdAt(Instant.now())
                .updatedAt(Instant.now())
                .build());

        assertThat(repository.findByState(LedgerState.FAILED_KAFKA))
                .isNotEmpty()
                .anyMatch(e -> e.getMessageId().equals("IT-MSG-003"));
    }

    @Test
    @DisplayName("should delete entry")
    void shouldDelete() {
        LedgerEntry entry = LedgerEntry.builder()
                .messageId("IT-MSG-004")
                .state(LedgerState.COMPLETED)
                .createdAt(Instant.now())
                .updatedAt(Instant.now())
                .build();

        repository.save(entry);
        assertThat(repository.existsByMessageId("IT-MSG-004")).isTrue();

        repository.delete("IT-MSG-004");
        assertThat(repository.existsByMessageId("IT-MSG-004")).isFalse();
    }
}
