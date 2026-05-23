package com.enterprise.bridge.ledger;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class HBaseLedgerRepositoryTest {

    private static final String TABLE_NAME = "bridge_ledger";
    private static final byte[] COLUMN_FAMILY = Bytes.toBytes("cf");

    @Mock
    private Connection connection;

    @Mock
    private Table table;

    private HBaseLedgerRepository repository;

    @BeforeEach
    void setUp() throws IOException {
        when(connection.getTable(TableName.valueOf(TABLE_NAME))).thenReturn(table);
        repository = new HBaseLedgerRepository(connection, TABLE_NAME);
    }

    @Nested
    @DisplayName("Save Operations")
    class SaveOperations {

        @Test
        @DisplayName("should save ledger entry to HBase")
        void shouldSaveLedgerEntry() throws IOException {
            LedgerEntry entry = createTestEntry("MSG-001", LedgerState.RECEIVED);

            repository.save(entry);

            ArgumentCaptor<Put> putCaptor = ArgumentCaptor.forClass(Put.class);
            verify(table).put(putCaptor.capture());

            Put capturedPut = putCaptor.getValue();
            assertThat(Bytes.toString(capturedPut.getRow())).isEqualTo("MSG-001");
        }

        @Test
        @DisplayName("should throw exception on save failure")
        void shouldThrowExceptionOnSaveFailure() throws IOException {
            LedgerEntry entry = createTestEntry("MSG-001", LedgerState.RECEIVED);
            doThrow(new IOException("HBase error")).when(table).put(any(Put.class));

            assertThatThrownBy(() -> repository.save(entry))
                    .isInstanceOf(HBaseLedgerRepository.LedgerPersistenceException.class)
                    .hasMessageContaining("Failed to save");
        }

        @Test
        @DisplayName("update should call save")
        void updateShouldCallSave() throws IOException {
            LedgerEntry entry = createTestEntry("MSG-001", LedgerState.KAFKA_PUBLISHED);

            repository.update(entry);

            verify(table).put(any(Put.class));
        }
    }

    @Nested
    @DisplayName("Find Operations")
    class FindOperations {

        @Test
        @DisplayName("should find entry by message ID")
        void shouldFindByMessageId() throws IOException {
            String messageId = "MSG-001";
            Result result = createMockResult(messageId, LedgerState.RECEIVED);
            when(table.get(any(Get.class))).thenReturn(result);

            Optional<LedgerEntry> found = repository.findByMessageId(messageId);

            assertThat(found).isPresent();
            assertThat(found.get().getMessageId()).isEqualTo(messageId);
            assertThat(found.get().getState()).isEqualTo(LedgerState.RECEIVED);
        }

        @Test
        @DisplayName("should return empty when entry not found")
        void shouldReturnEmptyWhenNotFound() throws IOException {
            Result emptyResult = mock(Result.class);
            when(emptyResult.isEmpty()).thenReturn(true);
            when(table.get(any(Get.class))).thenReturn(emptyResult);

            Optional<LedgerEntry> found = repository.findByMessageId("NONEXISTENT");

            assertThat(found).isEmpty();
        }

        @Test
        @DisplayName("should throw exception on find failure")
        void shouldThrowExceptionOnFindFailure() throws IOException {
            when(table.get(any(Get.class))).thenThrow(new IOException("HBase error"));

            assertThatThrownBy(() -> repository.findByMessageId("MSG-001"))
                    .isInstanceOf(HBaseLedgerRepository.LedgerPersistenceException.class)
                    .hasMessageContaining("Failed to find");
        }
    }

    @Nested
    @DisplayName("Scan Operations")
    class ScanOperations {

        @Test
        @DisplayName("should find entries by state")
        void shouldFindByState() throws IOException {
            ResultScanner scanner = mock(ResultScanner.class);
            Result result1 = createMockResult("MSG-001", LedgerState.FAILED_KAFKA);
            Result result2 = createMockResult("MSG-002", LedgerState.FAILED_KAFKA);
            when(scanner.iterator()).thenReturn(Arrays.asList(result1, result2).iterator());
            when(table.getScanner(any(Scan.class))).thenReturn(scanner);

            List<LedgerEntry> entries = repository.findByState(LedgerState.FAILED_KAFKA);

            assertThat(entries).hasSize(2);
        }

        @Test
        @DisplayName("should find recoverable entries")
        void shouldFindRecoverableEntries() throws IOException {
            ResultScanner emptyScanner = mock(ResultScanner.class);
            when(emptyScanner.iterator()).thenReturn(Collections.emptyIterator());

            ResultScanner failedKafkaScanner = mock(ResultScanner.class);
            Result result = createMockResult("MSG-001", LedgerState.FAILED_KAFKA);
            when(failedKafkaScanner.iterator()).thenReturn(Collections.singletonList(result).iterator());

            when(table.getScanner(any(Scan.class)))
                    .thenReturn(emptyScanner)
                    .thenReturn(emptyScanner)
                    .thenReturn(failedKafkaScanner)
                    .thenReturn(emptyScanner);

            List<LedgerEntry> entries = repository.findRecoverableEntries();

            assertThat(entries).hasSize(1);
        }

        @Test
        @DisplayName("should count entries by state")
        void shouldCountByState() throws IOException {
            ResultScanner scanner = mock(ResultScanner.class);
            Result result1 = createMockResult("MSG-001", LedgerState.COMPLETED);
            Result result2 = createMockResult("MSG-002", LedgerState.COMPLETED);
            Result result3 = createMockResult("MSG-003", LedgerState.COMPLETED);
            when(scanner.iterator()).thenReturn(Arrays.asList(result1, result2, result3).iterator());
            when(table.getScanner(any(Scan.class))).thenReturn(scanner);

            long count = repository.countByState(LedgerState.COMPLETED);

            assertThat(count).isEqualTo(3);
        }
    }

    @Nested
    @DisplayName("Existence Check")
    class ExistenceCheck {

        @Test
        @DisplayName("should return true when entry exists")
        void shouldReturnTrueWhenExists() throws IOException {
            when(table.exists(any(Get.class))).thenReturn(true);

            boolean exists = repository.existsByMessageId("MSG-001");

            assertThat(exists).isTrue();
        }

        @Test
        @DisplayName("should return false when entry does not exist")
        void shouldReturnFalseWhenNotExists() throws IOException {
            when(table.exists(any(Get.class))).thenReturn(false);

            boolean exists = repository.existsByMessageId("MSG-001");

            assertThat(exists).isFalse();
        }
    }

    @Nested
    @DisplayName("Delete Operations")
    class DeleteOperations {

        @Test
        @DisplayName("should delete entry from HBase")
        void shouldDeleteEntry() throws IOException {
            repository.delete("MSG-001");

            ArgumentCaptor<Delete> deleteCaptor = ArgumentCaptor.forClass(Delete.class);
            verify(table).delete(deleteCaptor.capture());

            Delete capturedDelete = deleteCaptor.getValue();
            assertThat(Bytes.toString(capturedDelete.getRow())).isEqualTo("MSG-001");
        }

        @Test
        @DisplayName("should throw exception on delete failure")
        void shouldThrowExceptionOnDeleteFailure() throws IOException {
            doThrow(new IOException("HBase error")).when(table).delete(any(Delete.class));

            assertThatThrownBy(() -> repository.delete("MSG-001"))
                    .isInstanceOf(HBaseLedgerRepository.LedgerPersistenceException.class)
                    .hasMessageContaining("Failed to delete");
        }
    }

    private LedgerEntry createTestEntry(String messageId, LedgerState state) {
        return LedgerEntry.builder()
                .messageId(messageId)
                .transactionId("TXN-001")
                .state(state)
                .hdfsPath("/data/test/path.json")
                .checksum("abc123")
                .kafkaOffset("0:100")
                .createdAt(Instant.now())
                .updatedAt(Instant.now())
                .retryCount(0)
                .lastError(null)
                .build();
    }

    private Result createMockResult(String messageId, LedgerState state) {
        Result result = mock(Result.class);
        when(result.isEmpty()).thenReturn(false);
        when(result.getRow()).thenReturn(Bytes.toBytes(messageId));
        when(result.getValue(COLUMN_FAMILY, Bytes.toBytes("bridge_message_id")))
                .thenReturn(Bytes.toBytes(messageId));
        when(result.getValue(COLUMN_FAMILY, Bytes.toBytes("original_mq_message_id")))
                .thenReturn(Bytes.toBytes("TXN-001"));
        when(result.getValue(COLUMN_FAMILY, Bytes.toBytes("status")))
                .thenReturn(Bytes.toBytes(state.name()));
        when(result.getValue(COLUMN_FAMILY, Bytes.toBytes("hdfs_path")))
                .thenReturn(Bytes.toBytes("/data/test/path.json"));
        when(result.getValue(COLUMN_FAMILY, Bytes.toBytes("checksum")))
                .thenReturn(Bytes.toBytes("abc123"));
        when(result.getValue(COLUMN_FAMILY, Bytes.toBytes("kafka_offset")))
                .thenReturn(Bytes.toBytes("0:100"));
        when(result.getValue(COLUMN_FAMILY, Bytes.toBytes("error_message")))
                .thenReturn(null);
        when(result.getValue(COLUMN_FAMILY, Bytes.toBytes("retry_count")))
                .thenReturn(Bytes.toBytes(0));
        when(result.getValue(COLUMN_FAMILY, Bytes.toBytes("created_at_utc")))
                .thenReturn(Bytes.toBytes(Instant.now().toEpochMilli()));
        when(result.getValue(COLUMN_FAMILY, Bytes.toBytes("updated_at_utc")))
                .thenReturn(Bytes.toBytes(Instant.now().toEpochMilli()));
        return result;
    }
}
