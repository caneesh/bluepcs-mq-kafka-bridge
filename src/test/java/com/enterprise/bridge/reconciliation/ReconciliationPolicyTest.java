package com.enterprise.bridge.reconciliation;

import com.enterprise.bridge.ledger.LedgerEntry;
import com.enterprise.bridge.ledger.LedgerRepository;
import com.enterprise.bridge.ledger.LedgerState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.time.Instant;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
@DisplayName("ReconciliationPolicy")
class ReconciliationPolicyTest {

    @Mock
    private LedgerRepository ledgerRepository;

    private ReconciliationPolicy policy;

    @BeforeEach
    void setUp() {
        policy = new ReconciliationPolicy(ledgerRepository);
    }

    @Nested
    @DisplayName("balanced reconciliation")
    class BalancedReconciliation {

        @Test
        @DisplayName("should return balanced result when counts match")
        void shouldReturnBalancedWhenCountsMatch() {
            when(ledgerRepository.countByState(LedgerState.COMPLETED)).thenReturn(100L);
            when(ledgerRepository.countByState(LedgerState.HDFS_WRITTEN)).thenReturn(0L);
            when(ledgerRepository.countByState(LedgerState.KAFKA_PUBLISHED)).thenReturn(0L);
            when(ledgerRepository.findByStates(any())).thenReturn(Collections.emptyList());

            ReconciliationResult result = policy.reconcile(100L, 100L);

            assertThat(result.isBalanced()).isTrue();
            assertThat(result.getLedgerCount()).isEqualTo(100L);
            assertThat(result.getHdfsCount()).isEqualTo(100L);
            assertThat(result.getKafkaCount()).isEqualTo(100L);
        }

        @Test
        @DisplayName("should return balanced result with in-flight messages")
        void shouldReturnBalancedWithInFlightMessages() {
            when(ledgerRepository.countByState(LedgerState.COMPLETED)).thenReturn(90L);
            when(ledgerRepository.countByState(LedgerState.HDFS_WRITTEN)).thenReturn(5L);
            when(ledgerRepository.countByState(LedgerState.KAFKA_PUBLISHED)).thenReturn(5L);
            when(ledgerRepository.findByStates(any())).thenReturn(Collections.emptyList());

            ReconciliationResult result = policy.reconcile(100L, 95L);

            assertThat(result.isBalanced()).isTrue();
        }

        @Test
        @DisplayName("should have empty findings when balanced")
        void shouldHaveEmptyFindingsWhenBalanced() {
            when(ledgerRepository.countByState(LedgerState.COMPLETED)).thenReturn(50L);
            when(ledgerRepository.countByState(LedgerState.HDFS_WRITTEN)).thenReturn(0L);
            when(ledgerRepository.countByState(LedgerState.KAFKA_PUBLISHED)).thenReturn(0L);
            when(ledgerRepository.findByStates(any())).thenReturn(Collections.emptyList());

            ReconciliationResult result = policy.reconcile(50L, 50L);

            assertThat(result.getFindings()).isEmpty();
        }
    }

    @Nested
    @DisplayName("mismatched counts")
    class MismatchedCounts {

        @Test
        @DisplayName("should return unbalanced result when HDFS count is lower than expected")
        void shouldReturnUnbalancedWhenHdfsCountLower() {
            when(ledgerRepository.countByState(LedgerState.COMPLETED)).thenReturn(100L);
            when(ledgerRepository.countByState(LedgerState.HDFS_WRITTEN)).thenReturn(0L);
            when(ledgerRepository.countByState(LedgerState.KAFKA_PUBLISHED)).thenReturn(0L);
            when(ledgerRepository.findByStates(any())).thenReturn(Collections.emptyList());

            ReconciliationResult result = policy.reconcile(95L, 100L);

            assertThat(result.isBalanced()).isFalse();
        }

        @Test
        @DisplayName("should return unbalanced result when Kafka count is lower than expected")
        void shouldReturnUnbalancedWhenKafkaCountLower() {
            when(ledgerRepository.countByState(LedgerState.COMPLETED)).thenReturn(100L);
            when(ledgerRepository.countByState(LedgerState.HDFS_WRITTEN)).thenReturn(0L);
            when(ledgerRepository.countByState(LedgerState.KAFKA_PUBLISHED)).thenReturn(0L);
            when(ledgerRepository.findByStates(any())).thenReturn(Collections.emptyList());

            ReconciliationResult result = policy.reconcile(100L, 95L);

            assertThat(result.isBalanced()).isFalse();
        }

        @Test
        @DisplayName("should return unbalanced when both counts are off")
        void shouldReturnUnbalancedWhenBothCountsOff() {
            when(ledgerRepository.countByState(LedgerState.COMPLETED)).thenReturn(100L);
            when(ledgerRepository.countByState(LedgerState.HDFS_WRITTEN)).thenReturn(0L);
            when(ledgerRepository.countByState(LedgerState.KAFKA_PUBLISHED)).thenReturn(0L);
            when(ledgerRepository.findByStates(any())).thenReturn(Collections.emptyList());

            ReconciliationResult result = policy.reconcile(90L, 80L);

            assertThat(result.isBalanced()).isFalse();
        }
    }

    @Nested
    @DisplayName("findings generation")
    class FindingsGeneration {

        @Test
        @DisplayName("should generate MISSING_IN_HDFS finding when HDFS count is low")
        void shouldGenerateMissingInHdfsFinding() {
            when(ledgerRepository.countByState(LedgerState.COMPLETED)).thenReturn(100L);
            when(ledgerRepository.countByState(LedgerState.HDFS_WRITTEN)).thenReturn(0L);
            when(ledgerRepository.countByState(LedgerState.KAFKA_PUBLISHED)).thenReturn(0L);
            when(ledgerRepository.findByStates(any())).thenReturn(Collections.emptyList());

            ReconciliationResult result = policy.reconcile(90L, 100L);

            assertThat(result.getFindings())
                    .extracting(ReconciliationFinding::getType)
                    .contains(ReconciliationFinding.FindingType.MISSING_IN_HDFS);
        }

        @Test
        @DisplayName("should include missing count in HDFS finding description")
        void shouldIncludeMissingCountInHdfsFinding() {
            when(ledgerRepository.countByState(LedgerState.COMPLETED)).thenReturn(100L);
            when(ledgerRepository.countByState(LedgerState.HDFS_WRITTEN)).thenReturn(0L);
            when(ledgerRepository.countByState(LedgerState.KAFKA_PUBLISHED)).thenReturn(0L);
            when(ledgerRepository.findByStates(any())).thenReturn(Collections.emptyList());

            ReconciliationResult result = policy.reconcile(90L, 100L);

            assertThat(result.getFindings())
                    .filteredOn(f -> f.getType() == ReconciliationFinding.FindingType.MISSING_IN_HDFS)
                    .extracting(ReconciliationFinding::getDescription)
                    .first()
                    .asString()
                    .contains("10");
        }

        @Test
        @DisplayName("should generate MISSING_IN_KAFKA finding when Kafka count is low")
        void shouldGenerateMissingInKafkaFinding() {
            when(ledgerRepository.countByState(LedgerState.COMPLETED)).thenReturn(100L);
            when(ledgerRepository.countByState(LedgerState.HDFS_WRITTEN)).thenReturn(0L);
            when(ledgerRepository.countByState(LedgerState.KAFKA_PUBLISHED)).thenReturn(0L);
            when(ledgerRepository.findByStates(any())).thenReturn(Collections.emptyList());

            ReconciliationResult result = policy.reconcile(100L, 95L);

            assertThat(result.getFindings())
                    .extracting(ReconciliationFinding::getType)
                    .contains(ReconciliationFinding.FindingType.MISSING_IN_KAFKA);
        }

        @Test
        @DisplayName("should generate ORPHAN_HDFS_FILE finding when HDFS count is higher")
        void shouldGenerateOrphanHdfsFileFinding() {
            when(ledgerRepository.countByState(LedgerState.COMPLETED)).thenReturn(100L);
            when(ledgerRepository.countByState(LedgerState.HDFS_WRITTEN)).thenReturn(0L);
            when(ledgerRepository.countByState(LedgerState.KAFKA_PUBLISHED)).thenReturn(0L);
            when(ledgerRepository.findByStates(any())).thenReturn(Collections.emptyList());

            ReconciliationResult result = policy.reconcile(110L, 100L);

            assertThat(result.getFindings())
                    .extracting(ReconciliationFinding::getType)
                    .contains(ReconciliationFinding.FindingType.ORPHAN_HDFS_FILE);
        }

        @Test
        @DisplayName("should generate ORPHAN_KAFKA_MESSAGE finding when Kafka count is higher")
        void shouldGenerateOrphanKafkaMessageFinding() {
            when(ledgerRepository.countByState(LedgerState.COMPLETED)).thenReturn(100L);
            when(ledgerRepository.countByState(LedgerState.HDFS_WRITTEN)).thenReturn(0L);
            when(ledgerRepository.countByState(LedgerState.KAFKA_PUBLISHED)).thenReturn(0L);
            when(ledgerRepository.findByStates(any())).thenReturn(Collections.emptyList());

            ReconciliationResult result = policy.reconcile(100L, 110L);

            assertThat(result.getFindings())
                    .extracting(ReconciliationFinding::getType)
                    .contains(ReconciliationFinding.FindingType.ORPHAN_KAFKA_MESSAGE);
        }

        @Test
        @DisplayName("should generate LEDGER_STALE finding for stuck entries")
        void shouldGenerateLedgerStaleFinding() {
            when(ledgerRepository.countByState(LedgerState.COMPLETED)).thenReturn(100L);
            when(ledgerRepository.countByState(LedgerState.HDFS_WRITTEN)).thenReturn(0L);
            when(ledgerRepository.countByState(LedgerState.KAFKA_PUBLISHED)).thenReturn(0L);

            LedgerEntry stuckEntry = LedgerEntry.builder()
                    .messageId("MSG-STUCK-001")
                    .state(LedgerState.ENRICHED)
                    .createdAt(Instant.now())
                    .updatedAt(Instant.now())
                    .build();
            when(ledgerRepository.findByStates(any())).thenReturn(List.of(stuckEntry));

            ReconciliationResult result = policy.reconcile(95L, 100L);

            assertThat(result.getFindings())
                    .extracting(ReconciliationFinding::getType)
                    .contains(ReconciliationFinding.FindingType.LEDGER_STALE);
        }

        @Test
        @DisplayName("should include message id in stale entry finding")
        void shouldIncludeMessageIdInStaleEntryFinding() {
            when(ledgerRepository.countByState(LedgerState.COMPLETED)).thenReturn(100L);
            when(ledgerRepository.countByState(LedgerState.HDFS_WRITTEN)).thenReturn(0L);
            when(ledgerRepository.countByState(LedgerState.KAFKA_PUBLISHED)).thenReturn(0L);

            LedgerEntry stuckEntry = LedgerEntry.builder()
                    .messageId("MSG-STUCK-002")
                    .state(LedgerState.HDFS_WRITTEN)
                    .createdAt(Instant.now())
                    .updatedAt(Instant.now())
                    .build();
            when(ledgerRepository.findByStates(any())).thenReturn(List.of(stuckEntry));

            ReconciliationResult result = policy.reconcile(95L, 100L);

            assertThat(result.getFindings())
                    .filteredOn(f -> f.getType() == ReconciliationFinding.FindingType.LEDGER_STALE)
                    .extracting(ReconciliationFinding::getMessageId)
                    .contains("MSG-STUCK-002");
        }

        @Test
        @DisplayName("should include state in stale entry description")
        void shouldIncludeStateInStaleEntryDescription() {
            when(ledgerRepository.countByState(LedgerState.COMPLETED)).thenReturn(100L);
            when(ledgerRepository.countByState(LedgerState.HDFS_WRITTEN)).thenReturn(0L);
            when(ledgerRepository.countByState(LedgerState.KAFKA_PUBLISHED)).thenReturn(0L);

            LedgerEntry stuckEntry = LedgerEntry.builder()
                    .messageId("MSG-STUCK-003")
                    .state(LedgerState.ENRICHED)
                    .createdAt(Instant.now())
                    .updatedAt(Instant.now())
                    .build();
            when(ledgerRepository.findByStates(any())).thenReturn(List.of(stuckEntry));

            ReconciliationResult result = policy.reconcile(95L, 100L);

            assertThat(result.getFindings())
                    .filteredOn(f -> f.getType() == ReconciliationFinding.FindingType.LEDGER_STALE)
                    .extracting(ReconciliationFinding::getDescription)
                    .first()
                    .asString()
                    .contains("ENRICHED");
        }

        @Test
        @DisplayName("should generate multiple findings for multiple issues")
        void shouldGenerateMultipleFindingsForMultipleIssues() {
            when(ledgerRepository.countByState(LedgerState.COMPLETED)).thenReturn(100L);
            when(ledgerRepository.countByState(LedgerState.HDFS_WRITTEN)).thenReturn(0L);
            when(ledgerRepository.countByState(LedgerState.KAFKA_PUBLISHED)).thenReturn(0L);

            LedgerEntry stuckEntry = LedgerEntry.builder()
                    .messageId("MSG-STUCK-004")
                    .state(LedgerState.ENRICHED)
                    .createdAt(Instant.now())
                    .updatedAt(Instant.now())
                    .build();
            when(ledgerRepository.findByStates(any())).thenReturn(List.of(stuckEntry));

            ReconciliationResult result = policy.reconcile(90L, 95L);

            assertThat(result.getFindings()).hasSizeGreaterThan(1);
            assertThat(result.getFindings())
                    .extracting(ReconciliationFinding::getType)
                    .contains(
                            ReconciliationFinding.FindingType.MISSING_IN_HDFS,
                            ReconciliationFinding.FindingType.MISSING_IN_KAFKA,
                            ReconciliationFinding.FindingType.LEDGER_STALE
                    );
        }
    }

    @Nested
    @DisplayName("result metadata")
    class ResultMetadata {

        @Test
        @DisplayName("should include timestamp in result")
        void shouldIncludeTimestampInResult() {
            when(ledgerRepository.countByState(LedgerState.COMPLETED)).thenReturn(50L);
            when(ledgerRepository.countByState(LedgerState.HDFS_WRITTEN)).thenReturn(0L);
            when(ledgerRepository.countByState(LedgerState.KAFKA_PUBLISHED)).thenReturn(0L);
            when(ledgerRepository.findByStates(any())).thenReturn(Collections.emptyList());

            Instant before = Instant.now();
            ReconciliationResult result = policy.reconcile(50L, 50L);
            Instant after = Instant.now();

            assertThat(result.getTimestamp()).isNotNull();
            assertThat(result.getTimestamp()).isAfterOrEqualTo(before);
            assertThat(result.getTimestamp()).isBeforeOrEqualTo(after);
        }

        @Test
        @DisplayName("should include all counts in result")
        void shouldIncludeAllCountsInResult() {
            when(ledgerRepository.countByState(LedgerState.COMPLETED)).thenReturn(80L);
            when(ledgerRepository.countByState(LedgerState.HDFS_WRITTEN)).thenReturn(10L);
            when(ledgerRepository.countByState(LedgerState.KAFKA_PUBLISHED)).thenReturn(10L);
            when(ledgerRepository.findByStates(any())).thenReturn(Collections.emptyList());

            ReconciliationResult result = policy.reconcile(90L, 80L);

            assertThat(result.getLedgerCount()).isEqualTo(80L);
            assertThat(result.getHdfsCount()).isEqualTo(90L);
            assertThat(result.getKafkaCount()).isEqualTo(80L);
        }
    }

    @Nested
    @DisplayName("edge cases")
    class EdgeCases {

        @Test
        @DisplayName("should handle zero counts")
        void shouldHandleZeroCounts() {
            when(ledgerRepository.countByState(LedgerState.COMPLETED)).thenReturn(0L);
            when(ledgerRepository.countByState(LedgerState.HDFS_WRITTEN)).thenReturn(0L);
            when(ledgerRepository.countByState(LedgerState.KAFKA_PUBLISHED)).thenReturn(0L);
            when(ledgerRepository.findByStates(any())).thenReturn(Collections.emptyList());

            ReconciliationResult result = policy.reconcile(0L, 0L);

            assertThat(result.isBalanced()).isTrue();
            assertThat(result.getLedgerCount()).isEqualTo(0L);
        }

        @Test
        @DisplayName("should handle large counts")
        void shouldHandleLargeCounts() {
            when(ledgerRepository.countByState(LedgerState.COMPLETED)).thenReturn(1000000L);
            when(ledgerRepository.countByState(LedgerState.HDFS_WRITTEN)).thenReturn(0L);
            when(ledgerRepository.countByState(LedgerState.KAFKA_PUBLISHED)).thenReturn(0L);
            when(ledgerRepository.findByStates(any())).thenReturn(Collections.emptyList());

            ReconciliationResult result = policy.reconcile(1000000L, 1000000L);

            assertThat(result.isBalanced()).isTrue();
            assertThat(result.getLedgerCount()).isEqualTo(1000000L);
        }

        @Test
        @DisplayName("should handle empty stale entries list")
        void shouldHandleEmptyStaleEntriesList() {
            when(ledgerRepository.countByState(LedgerState.COMPLETED)).thenReturn(100L);
            when(ledgerRepository.countByState(LedgerState.HDFS_WRITTEN)).thenReturn(0L);
            when(ledgerRepository.countByState(LedgerState.KAFKA_PUBLISHED)).thenReturn(0L);
            when(ledgerRepository.findByStates(any())).thenReturn(Collections.emptyList());

            ReconciliationResult result = policy.reconcile(95L, 100L);

            assertThat(result.getFindings())
                    .filteredOn(f -> f.getType() == ReconciliationFinding.FindingType.LEDGER_STALE)
                    .isEmpty();
        }
    }
}
