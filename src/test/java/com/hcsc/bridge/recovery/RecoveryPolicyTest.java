package com.hcsc.bridge.recovery;

import com.hcsc.bridge.ledger.LedgerEntry;
import com.hcsc.bridge.ledger.LedgerState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("RecoveryPolicy")
class RecoveryPolicyTest {

    private static final int MAX_RETRIES = 3;

    private RecoveryPolicy policy;

    @BeforeEach
    void setUp() {
        policy = new RecoveryPolicy(MAX_RETRIES);
    }

    @Nested
    @DisplayName("state: RECEIVED")
    class ReceivedState {

        @Test
        @DisplayName("should return SKIP action")
        void shouldReturnSkipAction() {
            LedgerEntry entry = createEntry("MSG-001", LedgerState.RECEIVED);

            RecoveryAction action = policy.determineAction(entry);

            assertThat(action).isEqualTo(RecoveryAction.SKIP);
        }

        @Test
        @DisplayName("should not be recoverable")
        void shouldNotBeRecoverable() {
            LedgerEntry entry = createEntry("MSG-001", LedgerState.RECEIVED);

            assertThat(policy.shouldRetry(entry)).isFalse();
        }
    }

    @Nested
    @DisplayName("state: PARSED")
    class ParsedState {

        @Test
        @DisplayName("should return SKIP action")
        void shouldReturnSkipAction() {
            LedgerEntry entry = createEntry("MSG-002", LedgerState.PARSED);

            RecoveryAction action = policy.determineAction(entry);

            assertThat(action).isEqualTo(RecoveryAction.SKIP);
        }
    }

    @Nested
    @DisplayName("state: ENRICHED")
    class EnrichedState {

        @Test
        @DisplayName("should return RETRY_HDFS_WRITE action")
        void shouldReturnRetryHdfsWriteAction() {
            LedgerEntry entry = createEntry("MSG-003", LedgerState.ENRICHED);

            RecoveryAction action = policy.determineAction(entry);

            assertThat(action).isEqualTo(RecoveryAction.RETRY_HDFS_WRITE);
        }

        @Test
        @DisplayName("should be recoverable")
        void shouldBeRecoverable() {
            LedgerEntry entry = createEntry("MSG-003", LedgerState.ENRICHED);

            assertThat(policy.shouldRetry(entry)).isTrue();
        }
    }

    @Nested
    @DisplayName("state: FAILED_HDFS")
    class FailedHdfsState {

        @Test
        @DisplayName("should return RETRY_HDFS_WRITE action")
        void shouldReturnRetryHdfsWriteAction() {
            LedgerEntry entry = createEntry("MSG-004", LedgerState.FAILED_HDFS);

            RecoveryAction action = policy.determineAction(entry);

            assertThat(action).isEqualTo(RecoveryAction.RETRY_HDFS_WRITE);
        }

        @Test
        @DisplayName("should be recoverable when under max retries")
        void shouldBeRecoverableUnderMaxRetries() {
            LedgerEntry entry = createEntryWithRetries("MSG-004", LedgerState.FAILED_HDFS, 1);

            assertThat(policy.shouldRetry(entry)).isTrue();
        }
    }

    @Nested
    @DisplayName("state: HDFS_WRITTEN")
    class HdfsWrittenState {

        @Test
        @DisplayName("should return RETRY_KAFKA_PUBLISH action")
        void shouldReturnRetryKafkaPublishAction() {
            LedgerEntry entry = createEntry("MSG-005", LedgerState.HDFS_WRITTEN);

            RecoveryAction action = policy.determineAction(entry);

            assertThat(action).isEqualTo(RecoveryAction.RETRY_KAFKA_PUBLISH);
        }

        @Test
        @DisplayName("should be recoverable")
        void shouldBeRecoverable() {
            LedgerEntry entry = createEntry("MSG-005", LedgerState.HDFS_WRITTEN);

            assertThat(policy.shouldRetry(entry)).isTrue();
        }
    }

    @Nested
    @DisplayName("state: FAILED_KAFKA")
    class FailedKafkaState {

        @Test
        @DisplayName("should return RETRY_KAFKA_PUBLISH action")
        void shouldReturnRetryKafkaPublishAction() {
            LedgerEntry entry = createEntry("MSG-006", LedgerState.FAILED_KAFKA);

            RecoveryAction action = policy.determineAction(entry);

            assertThat(action).isEqualTo(RecoveryAction.RETRY_KAFKA_PUBLISH);
        }

        @Test
        @DisplayName("should be recoverable")
        void shouldBeRecoverable() {
            LedgerEntry entry = createEntry("MSG-006", LedgerState.FAILED_KAFKA);

            assertThat(policy.shouldRetry(entry)).isTrue();
        }
    }

    @Nested
    @DisplayName("state: FAILED_ENRICHMENT")
    class FailedEnrichmentState {

        @Test
        @DisplayName("should return RETRY_ENRICHMENT action")
        void shouldReturnRetryEnrichmentAction() {
            LedgerEntry entry = createEntry("MSG-007", LedgerState.FAILED_ENRICHMENT);

            RecoveryAction action = policy.determineAction(entry);

            assertThat(action).isEqualTo(RecoveryAction.RETRY_ENRICHMENT);
        }

        @Test
        @DisplayName("should be recoverable")
        void shouldBeRecoverable() {
            LedgerEntry entry = createEntry("MSG-007", LedgerState.FAILED_ENRICHMENT);

            assertThat(policy.shouldRetry(entry)).isTrue();
        }
    }

    @Nested
    @DisplayName("state: FAILED_PARSE")
    class FailedParseState {

        @Test
        @DisplayName("should return MANUAL_INTERVENTION action")
        void shouldReturnManualInterventionAction() {
            LedgerEntry entry = createEntry("MSG-008", LedgerState.FAILED_PARSE);

            RecoveryAction action = policy.determineAction(entry);

            assertThat(action).isEqualTo(RecoveryAction.MANUAL_INTERVENTION);
        }

        @Test
        @DisplayName("should not be recoverable")
        void shouldNotBeRecoverable() {
            LedgerEntry entry = createEntry("MSG-008", LedgerState.FAILED_PARSE);

            assertThat(policy.shouldRetry(entry)).isFalse();
        }
    }

    @Nested
    @DisplayName("state: COMPLETED")
    class CompletedState {

        @Test
        @DisplayName("should return NO_ACTION")
        void shouldReturnNoAction() {
            LedgerEntry entry = createEntry("MSG-009", LedgerState.COMPLETED);

            RecoveryAction action = policy.determineAction(entry);

            assertThat(action).isEqualTo(RecoveryAction.NO_ACTION);
        }

        @Test
        @DisplayName("should not be recoverable")
        void shouldNotBeRecoverable() {
            LedgerEntry entry = createEntry("MSG-009", LedgerState.COMPLETED);

            assertThat(policy.shouldRetry(entry)).isFalse();
        }
    }

    @Nested
    @DisplayName("state: DUPLICATE_DETECTED")
    class DuplicateDetectedState {

        @Test
        @DisplayName("should return NO_ACTION")
        void shouldReturnNoAction() {
            LedgerEntry entry = createEntry("MSG-010", LedgerState.DUPLICATE_DETECTED);

            RecoveryAction action = policy.determineAction(entry);

            assertThat(action).isEqualTo(RecoveryAction.NO_ACTION);
        }
    }

    @Nested
    @DisplayName("state: KAFKA_PUBLISHED")
    class KafkaPublishedState {

        @Test
        @DisplayName("should return NO_ACTION")
        void shouldReturnNoAction() {
            LedgerEntry entry = createEntry("MSG-011", LedgerState.KAFKA_PUBLISHED);

            RecoveryAction action = policy.determineAction(entry);

            assertThat(action).isEqualTo(RecoveryAction.NO_ACTION);
        }
    }

    @Nested
    @DisplayName("state: RECOVERY_PENDING")
    class RecoveryPendingState {

        @Test
        @DisplayName("should return RETRY_KAFKA_PUBLISH when HDFS path exists but no kafka offset")
        void shouldReturnRetryKafkaWhenHdfsPathExists() {
            LedgerEntry entry = LedgerEntry.builder()
                    .messageId("MSG-RP-001")
                    .transactionId("TXN-001")
                    .state(LedgerState.RECOVERY_PENDING)
                    .hdfsPath("/path/file.json")
                    .kafkaOffset(null)
                    .createdAt(Instant.now())
                    .updatedAt(Instant.now())
                    .build();

            RecoveryAction action = policy.determineAction(entry);

            assertThat(action).isEqualTo(RecoveryAction.RETRY_KAFKA_PUBLISH);
        }

        @Test
        @DisplayName("should return RETRY_HDFS_WRITE when no HDFS path but has transaction id")
        void shouldReturnRetryHdfsWhenNoHdfsPath() {
            LedgerEntry entry = LedgerEntry.builder()
                    .messageId("MSG-RP-002")
                    .transactionId("TXN-001")
                    .state(LedgerState.RECOVERY_PENDING)
                    .hdfsPath(null)
                    .createdAt(Instant.now())
                    .updatedAt(Instant.now())
                    .build();

            RecoveryAction action = policy.determineAction(entry);

            assertThat(action).isEqualTo(RecoveryAction.RETRY_HDFS_WRITE);
        }

        @Test
        @DisplayName("should return SKIP when no transaction id")
        void shouldReturnSkipWhenNoTransactionId() {
            LedgerEntry entry = LedgerEntry.builder()
                    .messageId("MSG-RP-003")
                    .transactionId(null)
                    .state(LedgerState.RECOVERY_PENDING)
                    .createdAt(Instant.now())
                    .updatedAt(Instant.now())
                    .build();

            RecoveryAction action = policy.determineAction(entry);

            assertThat(action).isEqualTo(RecoveryAction.SKIP);
        }

        @Test
        @DisplayName("should return MANUAL_INTERVENTION when both HDFS and Kafka exist")
        void shouldReturnManualInterventionWhenBothExist() {
            LedgerEntry entry = LedgerEntry.builder()
                    .messageId("MSG-RP-004")
                    .transactionId("TXN-001")
                    .state(LedgerState.RECOVERY_PENDING)
                    .hdfsPath("/path/file.json")
                    .kafkaOffset("12345")
                    .createdAt(Instant.now())
                    .updatedAt(Instant.now())
                    .build();

            RecoveryAction action = policy.determineAction(entry);

            assertThat(action).isEqualTo(RecoveryAction.MANUAL_INTERVENTION);
        }
    }

    @Nested
    @DisplayName("max retries handling")
    class MaxRetriesHandling {

        @Test
        @DisplayName("should return MANUAL_INTERVENTION when max retries exceeded")
        void shouldReturnManualInterventionWhenMaxRetriesExceeded() {
            LedgerEntry entry = createEntryWithRetries("MSG-MAX-001", LedgerState.FAILED_HDFS, MAX_RETRIES);

            RecoveryAction action = policy.determineAction(entry);

            assertThat(action).isEqualTo(RecoveryAction.MANUAL_INTERVENTION);
        }

        @Test
        @DisplayName("should not be recoverable when max retries exceeded")
        void shouldNotBeRecoverableWhenMaxRetriesExceeded() {
            LedgerEntry entry = createEntryWithRetries("MSG-MAX-002", LedgerState.FAILED_KAFKA, MAX_RETRIES);

            assertThat(policy.shouldRetry(entry)).isFalse();
        }

        @Test
        @DisplayName("should be recoverable when under max retries")
        void shouldBeRecoverableUnderMaxRetries() {
            LedgerEntry entry = createEntryWithRetries("MSG-MAX-003", LedgerState.FAILED_KAFKA, MAX_RETRIES - 1);

            assertThat(policy.shouldRetry(entry)).isTrue();
        }

        @ParameterizedTest
        @EnumSource(value = LedgerState.class, names = {"FAILED_HDFS", "FAILED_KAFKA", "FAILED_ENRICHMENT"})
        @DisplayName("should return MANUAL_INTERVENTION for any failed state when max retries exceeded")
        void shouldReturnManualInterventionForAnyFailedStateWhenMaxRetriesExceeded(LedgerState state) {
            LedgerEntry entry = createEntryWithRetries("MSG-MAX-004", state, MAX_RETRIES);

            RecoveryAction action = policy.determineAction(entry);

            assertThat(action).isEqualTo(RecoveryAction.MANUAL_INTERVENTION);
        }
    }

    @Nested
    @DisplayName("null entry handling")
    class NullEntryHandling {

        @Test
        @DisplayName("should return NO_ACTION for null entry")
        void shouldReturnNoActionForNullEntry() {
            RecoveryAction action = policy.determineAction(null);

            assertThat(action).isEqualTo(RecoveryAction.NO_ACTION);
        }
    }

    @Nested
    @DisplayName("recovery action expectations")
    class RecoveryActionExpectations {

        @ParameterizedTest
        @EnumSource(value = LedgerState.class, names = {"COMPLETED", "DUPLICATE_DETECTED", "KAFKA_PUBLISHED"})
        @DisplayName("terminal states should not require recovery")
        void terminalStatesShouldNotRequireRecovery(LedgerState state) {
            LedgerEntry entry = createEntry("MSG-TERM", state);

            RecoveryAction action = policy.determineAction(entry);

            assertThat(action).isEqualTo(RecoveryAction.NO_ACTION);
        }

        @ParameterizedTest
        @EnumSource(value = LedgerState.class, names = {"ENRICHED", "FAILED_HDFS"})
        @DisplayName("HDFS-recoverable states should return RETRY_HDFS_WRITE")
        void hdfsRecoverableStatesShouldReturnRetryHdfsWrite(LedgerState state) {
            LedgerEntry entry = createEntry("MSG-HDFS", state);

            RecoveryAction action = policy.determineAction(entry);

            assertThat(action).isEqualTo(RecoveryAction.RETRY_HDFS_WRITE);
        }

        @ParameterizedTest
        @EnumSource(value = LedgerState.class, names = {"HDFS_WRITTEN", "FAILED_KAFKA"})
        @DisplayName("Kafka-recoverable states should return RETRY_KAFKA_PUBLISH")
        void kafkaRecoverableStatesShouldReturnRetryKafkaPublish(LedgerState state) {
            LedgerEntry entry = createEntry("MSG-KAFKA", state);

            RecoveryAction action = policy.determineAction(entry);

            assertThat(action).isEqualTo(RecoveryAction.RETRY_KAFKA_PUBLISH);
        }
    }

    private LedgerEntry createEntry(String messageId, LedgerState state) {
        return createEntryWithRetries(messageId, state, 0);
    }

    private LedgerEntry createEntryWithRetries(String messageId, LedgerState state, int retryCount) {
        return LedgerEntry.builder()
                .messageId(messageId)
                .transactionId("TXN-" + messageId)
                .state(state)
                .retryCount(retryCount)
                .createdAt(Instant.now())
                .updatedAt(Instant.now())
                .build();
    }
}
