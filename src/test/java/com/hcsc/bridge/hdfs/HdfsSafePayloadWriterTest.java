package com.hcsc.bridge.hdfs;

import com.hcsc.bridge.core.ProcessingContext;
import com.hcsc.bridge.model.EnrichedPayload;
import com.hcsc.bridge.model.HdfsWriteResult;
import com.hcsc.bridge.model.ParsedPayload;
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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.endsWith;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
@DisplayName("HdfsSafePayloadWriter")
class HdfsSafePayloadWriterTest {

    private static final String BASE_PATH = "/data/bridge/payloads";
    private static final String TEMP_SUFFIX = ".tmp";
    private static final String WRAPPER_CONTENT = "{\"changeEventTimeStamp\":\"\",\"RestAPIResponse\":{},\"changeEventTypeName\":\"Unknown\"}";

    @Mock
    private HdfsFileOperations hdfsFileOperations;

    private HdfsSafePayloadWriter writer;

    @BeforeEach
    void setUp() {
        writer = new HdfsSafePayloadWriter(hdfsFileOperations, BASE_PATH, "", TEMP_SUFFIX);
    }

    @Nested
    @DisplayName("successful write operations")
    class SuccessfulWrite {

        @Test
        @DisplayName("should write payload and return success result")
        void shouldWritePayloadSuccessfully() throws IOException {
            EnrichedPayload payload = createEnrichedPayload("MSG-001", "TXN-001", "event-id-001");
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

            when(hdfsFileOperations.exists(anyString())).thenReturn(false);
            when(hdfsFileOperations.create(endsWith(TEMP_SUFFIX))).thenReturn(outputStream);
            when(hdfsFileOperations.rename(anyString(), anyString())).thenReturn(true);
            when(hdfsFileOperations.getFileChecksum(anyString())).thenAnswer(inv -> {
                byte[] content = outputStream.toByteArray();
                return calculateChecksum(content);
            });
            doNothing().when(hdfsFileOperations).mkdirs(anyString());

            HdfsWriteResult result = writer.write(payload, WRAPPER_CONTENT);

            assertThat(result.isNewWrite()).isTrue();
            assertThat(result.isAlreadyExists()).isFalse();
            assertThat(result.getHdfsPath()).contains(BASE_PATH);
            // Flat landing directory: no eventType/date partitioning in the path
            assertThat(result.getHdfsPath()).isEqualTo(BASE_PATH + "/event-id-001.json");
            assertThat(result.getChecksum()).isNotEmpty();
            assertThat(result.getBytesWritten()).isGreaterThan(0);
        }

        @Test
        @DisplayName("should tolerate a trailing slash on the base path")
        void shouldTolerateTrailingSlashOnBasePath() throws IOException {
            HdfsSafePayloadWriter slashWriter =
                    new HdfsSafePayloadWriter(hdfsFileOperations, BASE_PATH + "/", "", TEMP_SUFFIX);
            EnrichedPayload payload = createEnrichedPayload("MSG-SLASH", "TXN-SLASH", "event-id-slash");
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

            when(hdfsFileOperations.exists(anyString())).thenReturn(false);
            when(hdfsFileOperations.create(endsWith(TEMP_SUFFIX))).thenReturn(outputStream);
            when(hdfsFileOperations.rename(anyString(), anyString())).thenReturn(true);
            when(hdfsFileOperations.getFileChecksum(anyString())).thenAnswer(inv ->
                    calculateChecksum(outputStream.toByteArray()));
            doNothing().when(hdfsFileOperations).mkdirs(anyString());

            HdfsWriteResult result = slashWriter.write(payload, WRAPPER_CONTENT);

            assertThat(result.getHdfsPath()).isEqualTo(BASE_PATH + "/event-id-slash.json");
            assertThat(result.getHdfsPath()).doesNotContain("//");
        }

        @Test
        @DisplayName("should use eventId in file path")
        void shouldUseEventIdInFilePath() throws IOException {
            EnrichedPayload payload = createEnrichedPayload("MSG-002", "TXN-002", "deterministic-event-id");
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

            when(hdfsFileOperations.exists(anyString())).thenReturn(false);
            when(hdfsFileOperations.create(anyString())).thenReturn(outputStream);
            when(hdfsFileOperations.rename(anyString(), anyString())).thenReturn(true);
            when(hdfsFileOperations.getFileChecksum(anyString())).thenAnswer(inv -> {
                return calculateChecksum(outputStream.toByteArray());
            });
            doNothing().when(hdfsFileOperations).mkdirs(anyString());

            HdfsWriteResult result = writer.write(payload, WRAPPER_CONTENT);

            assertThat(result.getHdfsPath()).contains("deterministic-event-id.json");
        }

        @Test
        @DisplayName("should create parent directories")
        void shouldCreateParentDirectories() throws IOException {
            EnrichedPayload payload = createEnrichedPayload("MSG-002", "TXN-002", "event-id-002");
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

            when(hdfsFileOperations.exists(anyString())).thenReturn(false);
            when(hdfsFileOperations.create(anyString())).thenReturn(outputStream);
            when(hdfsFileOperations.rename(anyString(), anyString())).thenReturn(true);
            when(hdfsFileOperations.getFileChecksum(anyString())).thenAnswer(inv -> {
                return calculateChecksum(outputStream.toByteArray());
            });
            doNothing().when(hdfsFileOperations).mkdirs(anyString());

            writer.write(payload, WRAPPER_CONTENT);

            ArgumentCaptor<String> pathCaptor = ArgumentCaptor.forClass(String.class);
            verify(hdfsFileOperations).mkdirs(pathCaptor.capture());
            assertThat(pathCaptor.getValue()).startsWith(BASE_PATH);
        }

        @Test
        @DisplayName("should write to temp file first then rename")
        void shouldWriteToTempThenRename() throws IOException {
            EnrichedPayload payload = createEnrichedPayload("MSG-003", "TXN-003", "event-id-003");
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

            when(hdfsFileOperations.exists(anyString())).thenReturn(false);
            when(hdfsFileOperations.create(endsWith(TEMP_SUFFIX))).thenReturn(outputStream);
            when(hdfsFileOperations.rename(anyString(), anyString())).thenReturn(true);
            when(hdfsFileOperations.getFileChecksum(anyString())).thenAnswer(inv -> {
                return calculateChecksum(outputStream.toByteArray());
            });
            doNothing().when(hdfsFileOperations).mkdirs(anyString());

            writer.write(payload, WRAPPER_CONTENT);

            ArgumentCaptor<String> sourceCaptor = ArgumentCaptor.forClass(String.class);
            ArgumentCaptor<String> targetCaptor = ArgumentCaptor.forClass(String.class);
            verify(hdfsFileOperations).rename(sourceCaptor.capture(), targetCaptor.capture());

            assertThat(sourceCaptor.getValue()).endsWith(TEMP_SUFFIX);
            assertThat(targetCaptor.getValue()).doesNotEndWith(TEMP_SUFFIX);
            assertThat(targetCaptor.getValue()).endsWith(".json");
        }
    }

    @Nested
    @DisplayName("existing file handling")
    class ExistingFileHandling {

        @Test
        @DisplayName("should skip write when file already exists")
        void shouldSkipWriteWhenFileExists() throws IOException {
            EnrichedPayload payload = createEnrichedPayload("MSG-DUP-001", "TXN-DUP-001", "event-id-dup-001");
            String existingChecksum = "existing-checksum-hash";

            when(hdfsFileOperations.exists(anyString())).thenReturn(true);
            when(hdfsFileOperations.getFileChecksum(anyString())).thenReturn(existingChecksum);

            HdfsWriteResult result = writer.write(payload, WRAPPER_CONTENT);

            assertThat(result.isAlreadyExists()).isTrue();
            assertThat(result.isNewWrite()).isFalse();
            assertThat(result.getChecksum()).isEqualTo(existingChecksum);
            assertThat(result.getBytesWritten()).isZero();

            verify(hdfsFileOperations, never()).create(anyString());
            verify(hdfsFileOperations, never()).rename(anyString(), anyString());
        }

        @Test
        @DisplayName("should return existing path when file exists")
        void shouldReturnExistingPath() throws IOException {
            EnrichedPayload payload = createEnrichedPayload("MSG-DUP-002", "TXN-DUP-002", "event-id-dup-002");

            when(hdfsFileOperations.exists(anyString())).thenReturn(true);
            when(hdfsFileOperations.getFileChecksum(anyString())).thenReturn("checksum");

            HdfsWriteResult result = writer.write(payload, WRAPPER_CONTENT);

            assertThat(result.getHdfsPath()).contains(BASE_PATH);
            assertThat(result.getHdfsPath()).contains("event-id-dup-002");
        }
    }

    @Nested
    @DisplayName("temp file cleanup")
    class TempFileCleanup {

        @Test
        @DisplayName("should cleanup temp file on write failure")
        void shouldCleanupTempFileOnWriteFailure() throws IOException {
            EnrichedPayload payload = createEnrichedPayload("MSG-FAIL-001", "TXN-FAIL-001", "event-id-fail-001");
            OutputStream failingStream = new OutputStream() {
                @Override
                public void write(int b) throws IOException {
                    throw new IOException("Simulated write failure");
                }
            };

            when(hdfsFileOperations.exists(contains(".json"))).thenReturn(false);
            when(hdfsFileOperations.exists(endsWith(TEMP_SUFFIX))).thenReturn(true);
            when(hdfsFileOperations.create(anyString())).thenReturn(failingStream);
            doNothing().when(hdfsFileOperations).mkdirs(anyString());
            doNothing().when(hdfsFileOperations).delete(anyString());

            assertThatThrownBy(() -> writer.write(payload, WRAPPER_CONTENT))
                    .isInstanceOf(HdfsWriteException.class);

            verify(hdfsFileOperations).delete(endsWith(TEMP_SUFFIX));
        }

        @Test
        @DisplayName("should cleanup temp file on rename failure")
        void shouldCleanupTempFileOnRenameFailure() throws IOException {
            EnrichedPayload payload = createEnrichedPayload("MSG-FAIL-002", "TXN-FAIL-002", "event-id-fail-002");
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

            when(hdfsFileOperations.exists(contains(".json"))).thenReturn(false);
            when(hdfsFileOperations.exists(endsWith(TEMP_SUFFIX))).thenReturn(true);
            when(hdfsFileOperations.create(anyString())).thenReturn(outputStream);
            when(hdfsFileOperations.rename(anyString(), anyString())).thenReturn(false);
            doNothing().when(hdfsFileOperations).mkdirs(anyString());
            doNothing().when(hdfsFileOperations).delete(anyString());

            assertThatThrownBy(() -> writer.write(payload, WRAPPER_CONTENT))
                    .isInstanceOf(HdfsWriteException.class)
                    .hasMessageContaining("Failed to rename temp file");

            verify(hdfsFileOperations).delete(endsWith(TEMP_SUFFIX));
        }

        @Test
        @DisplayName("should handle cleanup failure gracefully")
        void shouldHandleCleanupFailureGracefully() throws IOException {
            EnrichedPayload payload = createEnrichedPayload("MSG-FAIL-003", "TXN-FAIL-003", "event-id-fail-003");
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

            when(hdfsFileOperations.exists(contains(".json"))).thenReturn(false);
            when(hdfsFileOperations.exists(endsWith(TEMP_SUFFIX))).thenReturn(true);
            when(hdfsFileOperations.create(anyString())).thenReturn(outputStream);
            when(hdfsFileOperations.rename(anyString(), anyString())).thenReturn(false);
            doNothing().when(hdfsFileOperations).mkdirs(anyString());
            doThrow(new IOException("Delete failed")).when(hdfsFileOperations).delete(anyString());

            assertThatThrownBy(() -> writer.write(payload, WRAPPER_CONTENT))
                    .isInstanceOf(HdfsWriteException.class)
                    .hasMessageContaining("Failed to rename temp file");
        }
    }

    @Nested
    @DisplayName("rename failure handling")
    class RenameFailure {

        @Test
        @DisplayName("should throw exception when rename returns false")
        void shouldThrowWhenRenameFails() throws IOException {
            EnrichedPayload payload = createEnrichedPayload("MSG-REN-001", "TXN-REN-001", "event-id-ren-001");
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

            when(hdfsFileOperations.exists(anyString())).thenReturn(false);
            when(hdfsFileOperations.create(anyString())).thenReturn(outputStream);
            when(hdfsFileOperations.rename(anyString(), anyString())).thenReturn(false);
            doNothing().when(hdfsFileOperations).mkdirs(anyString());
            doNothing().when(hdfsFileOperations).delete(anyString());

            assertThatThrownBy(() -> writer.write(payload, WRAPPER_CONTENT))
                    .isInstanceOf(HdfsWriteException.class)
                    .hasMessageContaining("Failed to rename");
        }

        @Test
        @DisplayName("should include target path in exception")
        void shouldIncludeTargetPathInException() throws IOException {
            EnrichedPayload payload = createEnrichedPayload("MSG-REN-002", "TXN-REN-002", "event-id-ren-002");
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

            when(hdfsFileOperations.exists(anyString())).thenReturn(false);
            when(hdfsFileOperations.create(anyString())).thenReturn(outputStream);
            when(hdfsFileOperations.rename(anyString(), anyString())).thenReturn(false);
            doNothing().when(hdfsFileOperations).mkdirs(anyString());
            doNothing().when(hdfsFileOperations).delete(anyString());

            assertThatThrownBy(() -> writer.write(payload, WRAPPER_CONTENT))
                    .isInstanceOf(HdfsWriteException.class)
                    .extracting("targetPath")
                    .asString()
                    .contains(BASE_PATH);
        }
    }

    @Nested
    @DisplayName("checksum validation")
    class ChecksumValidation {

        @Test
        @DisplayName("should validate checksum after write")
        void shouldValidateChecksumAfterWrite() throws IOException {
            EnrichedPayload payload = createEnrichedPayload("MSG-CHK-001", "TXN-CHK-001", "event-id-chk-001");
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

            when(hdfsFileOperations.exists(anyString())).thenReturn(false);
            when(hdfsFileOperations.create(anyString())).thenReturn(outputStream);
            when(hdfsFileOperations.rename(anyString(), anyString())).thenReturn(true);
            when(hdfsFileOperations.getFileChecksum(anyString())).thenAnswer(inv -> {
                byte[] content = outputStream.toByteArray();
                return calculateChecksum(content);
            });
            doNothing().when(hdfsFileOperations).mkdirs(anyString());

            HdfsWriteResult result = writer.write(payload, WRAPPER_CONTENT);

            assertThat(result.getChecksum()).isNotEmpty();
            verify(hdfsFileOperations).getFileChecksum(anyString());
        }

        @Test
        @DisplayName("should throw exception on checksum mismatch")
        void shouldThrowOnChecksumMismatch() throws IOException {
            EnrichedPayload payload = createEnrichedPayload("MSG-CHK-002", "TXN-CHK-002", "event-id-chk-002");
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

            when(hdfsFileOperations.exists(anyString())).thenReturn(false);
            when(hdfsFileOperations.create(anyString())).thenReturn(outputStream);
            when(hdfsFileOperations.rename(anyString(), anyString())).thenReturn(true);
            when(hdfsFileOperations.getFileChecksum(anyString())).thenReturn("different-checksum");
            doNothing().when(hdfsFileOperations).mkdirs(anyString());

            assertThatThrownBy(() -> writer.write(payload, WRAPPER_CONTENT))
                    .isInstanceOf(HdfsWriteException.class)
                    .hasMessageContaining("Checksum mismatch");
        }
    }

    @Nested
    @DisplayName("quarantine writes")
    class QuarantineWrite {

        private void stubSuccessfulWrite(ByteArrayOutputStream outputStream) throws IOException {
            when(hdfsFileOperations.exists(anyString())).thenReturn(false);
            when(hdfsFileOperations.create(anyString())).thenReturn(outputStream);
            when(hdfsFileOperations.rename(anyString(), anyString())).thenReturn(true);
            when(hdfsFileOperations.getFileChecksum(anyString())).thenAnswer(inv ->
                    calculateChecksum(outputStream.toByteArray()));
            doNothing().when(hdfsFileOperations).mkdirs(anyString());
        }

        @Test
        @DisplayName("should write raw payload to default errors directory under base path")
        void shouldWriteToDefaultErrorsDirectory() throws IOException {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            stubSuccessfulWrite(outputStream);

            HdfsWriteResult result = writer.writeQuarantine("event-id-q1", "not json", "MSG-Q1");

            assertThat(result.isNewWrite()).isTrue();
            assertThat(result.getHdfsPath()).isEqualTo(BASE_PATH + "/errors/event-id-q1.json");
            assertThat(outputStream.toString("UTF-8")).isEqualTo("not json");
        }

        @Test
        @DisplayName("should use explicitly configured error path")
        void shouldUseConfiguredErrorPath() throws IOException {
            HdfsSafePayloadWriter customWriter = new HdfsSafePayloadWriter(
                    hdfsFileOperations, BASE_PATH, "/custom/quarantine/", TEMP_SUFFIX);
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            stubSuccessfulWrite(outputStream);

            HdfsWriteResult result = customWriter.writeQuarantine("event-id-q2", "{}", "MSG-Q2");

            // Trailing slash tolerated, no "//" in the advertised path
            assertThat(result.getHdfsPath()).isEqualTo("/custom/quarantine/event-id-q2.json");
        }

        @Test
        @DisplayName("should tolerate a null raw payload")
        void shouldTolerateNullPayload() throws IOException {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            stubSuccessfulWrite(outputStream);

            HdfsWriteResult result = writer.writeQuarantine("event-id-q3", null, "MSG-Q3");

            assertThat(result.isNewWrite()).isTrue();
            assertThat(result.getBytesWritten()).isZero();
        }

        @Test
        @DisplayName("should be idempotent: existing quarantine file is not rewritten")
        void shouldSkipExistingQuarantineFile() throws IOException {
            when(hdfsFileOperations.exists(BASE_PATH + "/errors/event-id-q4.json")).thenReturn(true);
            when(hdfsFileOperations.getFileChecksum(anyString())).thenReturn("existing-checksum");

            HdfsWriteResult result = writer.writeQuarantine("event-id-q4", "not json", "MSG-Q4");

            assertThat(result.isAlreadyExists()).isTrue();
            verify(hdfsFileOperations, never()).create(anyString());
        }
    }

    private EnrichedPayload createEnrichedPayload(String messageId, String transactionId, String eventId) {
        ParsedPayload parsed = new ParsedPayload(
                messageId,
                transactionId,
                "ORDER_CREATED",
                "ENT-001",
                "2026-01-01",
                Map.of("key", "value"),
                Instant.now(),
                "{\"test\":\"payload\"}"
        );
        ProcessingContext ctx = new ProcessingContext(eventId, messageId, Instant.now());
        return new EnrichedPayload(
                parsed,
                ctx,
                Map.of("enriched", "data"),
                "MP-001",
                "CAMP-001",
                Instant.now()
        );
    }

    private String calculateChecksum(byte[] content) {
        try {
            java.security.MessageDigest digest = java.security.MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(content);
            StringBuilder sb = new StringBuilder();
            for (byte b : hash) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }
}
