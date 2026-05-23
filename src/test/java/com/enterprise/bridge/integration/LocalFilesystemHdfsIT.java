package com.enterprise.bridge.integration;

import com.enterprise.bridge.hdfs.HdfsSafePayloadWriter;
import com.enterprise.bridge.hdfs.HdfsWriteException;
import com.enterprise.bridge.mock.LocalFileSystemHdfsOperations;
import com.enterprise.bridge.mock.SamplePayloadGenerator;
import com.enterprise.bridge.model.EnrichedPayload;
import com.enterprise.bridge.model.HdfsWriteResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LocalFilesystemHdfsIT {

    private LocalFileSystemHdfsOperations hdfsOperations;
    private HdfsSafePayloadWriter payloadWriter;
    private SamplePayloadGenerator payloadGenerator;
    private Path tempDir;

    @BeforeEach
    void setUp() throws IOException {
        tempDir = Files.createTempDirectory("hdfs-integration-test");
        hdfsOperations = new LocalFileSystemHdfsOperations(tempDir);
        payloadWriter = new HdfsSafePayloadWriter(hdfsOperations, "/data/bridge/payloads", ".tmp");
        payloadGenerator = new SamplePayloadGenerator();
    }

    @AfterEach
    void tearDown() throws IOException {
        hdfsOperations.cleanup();
    }

    @Nested
    @DisplayName("Temp File Handling Tests")
    class TempFileHandlingTests {

        @Test
        @DisplayName("should not leave temp files after successful write")
        void shouldNotLeaveTempFiles() throws IOException {
            EnrichedPayload payload = payloadGenerator.generateEnrichedPayload();

            HdfsWriteResult result = payloadWriter.write(payload);

            assertThat(result.isNewWrite()).isTrue();
            assertThat(hdfsOperations.exists(result.getHdfsPath())).isTrue();
            assertThat(hdfsOperations.exists(result.getHdfsPath() + ".tmp")).isFalse();
        }

        @Test
        @DisplayName("should cleanup temp file on rename failure")
        void shouldCleanupTempFileOnRenameFailure() throws IOException {
            EnrichedPayload payload = payloadGenerator.generateEnrichedPayload();
            hdfsOperations.setShouldFailOnRename(true);
            hdfsOperations.setFailureMessage("Rename failed");

            assertThatThrownBy(() -> payloadWriter.write(payload))
                    .isInstanceOf(HdfsWriteException.class);

            long tmpFileCount = Files.walk(tempDir)
                    .filter(p -> p.toString().endsWith(".tmp"))
                    .count();
            assertThat(tmpFileCount).isEqualTo(0);
        }

        @Test
        @DisplayName("should cleanup temp file on write failure")
        void shouldCleanupTempFileOnWriteFailure() throws IOException {
            EnrichedPayload payload = payloadGenerator.generateEnrichedPayload();

            payloadWriter.write(payload);

            hdfsOperations.setShouldFailOnCreate(true);
            EnrichedPayload payload2 = payloadGenerator.generateEnrichedPayload();

            assertThatThrownBy(() -> payloadWriter.write(payload2))
                    .isInstanceOf(HdfsWriteException.class);

            long tmpFileCount = Files.walk(tempDir)
                    .filter(p -> p.toString().endsWith(".tmp"))
                    .count();
            assertThat(tmpFileCount).isEqualTo(0);
        }
    }

    @Nested
    @DisplayName("Rename Semantics Tests")
    class RenameSemanticsTests {

        @Test
        @DisplayName("should write to temp path first then rename")
        void shouldWriteToTempThenRename() {
            EnrichedPayload payload = payloadGenerator.generateEnrichedPayload();

            HdfsWriteResult result = payloadWriter.write(payload);

            assertThat(result.getHdfsPath()).doesNotContain(".tmp");
            assertThat(result.isNewWrite()).isTrue();
        }

        @Test
        @DisplayName("should verify checksum after rename")
        void shouldVerifyChecksumAfterRename() {
            EnrichedPayload payload = payloadGenerator.generateEnrichedPayload();

            HdfsWriteResult result = payloadWriter.write(payload);

            assertThat(result.getChecksum()).isNotNull();
            assertThat(result.getChecksum()).hasSize(64);
        }

        @Test
        @DisplayName("should create parent directories")
        void shouldCreateParentDirectories() throws IOException {
            EnrichedPayload payload = payloadGenerator.generateEnrichedPayload();

            HdfsWriteResult result = payloadWriter.write(payload);

            Path finalPath = tempDir.resolve(result.getHdfsPath().substring(1));
            assertThat(Files.exists(finalPath.getParent())).isTrue();
        }
    }

    @Nested
    @DisplayName("Duplicate File Prevention Tests")
    class DuplicateFilePreventionTests {

        @Test
        @DisplayName("should return alreadyExists for duplicate writes")
        void shouldReturnAlreadyExistsForDuplicates() {
            EnrichedPayload payload = payloadGenerator.generateEnrichedPayload();

            HdfsWriteResult result1 = payloadWriter.write(payload);
            HdfsWriteResult result2 = payloadWriter.write(payload);

            assertThat(result1.isNewWrite()).isTrue();
            assertThat(result2.isAlreadyExists()).isTrue();
            assertThat(result2.getHdfsPath()).isEqualTo(result1.getHdfsPath());
        }

        @Test
        @DisplayName("should return existing checksum for duplicate")
        void shouldReturnExistingChecksumForDuplicate() {
            EnrichedPayload payload = payloadGenerator.generateEnrichedPayload();

            HdfsWriteResult result1 = payloadWriter.write(payload);
            HdfsWriteResult result2 = payloadWriter.write(payload);

            assertThat(result2.getChecksum()).isNotNull();
        }

        @Test
        @DisplayName("should not overwrite existing file")
        void shouldNotOverwriteExistingFile() throws IOException {
            EnrichedPayload payload = payloadGenerator.generateEnrichedPayload();

            HdfsWriteResult result1 = payloadWriter.write(payload);
            String originalContent = hdfsOperations.readFile(result1.getHdfsPath());

            HdfsWriteResult result2 = payloadWriter.write(payload);
            String contentAfterSecondWrite = hdfsOperations.readFile(result2.getHdfsPath());

            assertThat(contentAfterSecondWrite).isEqualTo(originalContent);
        }
    }

    @Nested
    @DisplayName("Concurrent Write Tests")
    class ConcurrentWriteTests {

        @Test
        @DisplayName("should handle concurrent writes to different files")
        void shouldHandleConcurrentWrites() throws InterruptedException {
            int threadCount = 5;
            ExecutorService executor = Executors.newFixedThreadPool(threadCount);
            CountDownLatch latch = new CountDownLatch(threadCount);
            AtomicInteger successCount = new AtomicInteger(0);

            for (int i = 0; i < threadCount; i++) {
                executor.submit(() -> {
                    try {
                        EnrichedPayload payload = payloadGenerator.generateEnrichedPayload();
                        HdfsWriteResult result = payloadWriter.write(payload);
                        if (result.isNewWrite()) {
                            successCount.incrementAndGet();
                        }
                    } finally {
                        latch.countDown();
                    }
                });
            }

            latch.await();
            executor.shutdown();

            assertThat(successCount.get()).isEqualTo(threadCount);
        }

        @Test
        @DisplayName("should handle concurrent writes to same file gracefully")
        void shouldHandleConcurrentWritesToSameFile() throws InterruptedException {
            int threadCount = 5;
            ExecutorService executor = Executors.newFixedThreadPool(threadCount);
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch doneLatch = new CountDownLatch(threadCount);
            AtomicInteger newWriteCount = new AtomicInteger(0);
            AtomicInteger duplicateCount = new AtomicInteger(0);
            AtomicInteger errorCount = new AtomicInteger(0);

            EnrichedPayload sharedPayload = payloadGenerator.generateEnrichedPayload();

            for (int i = 0; i < threadCount; i++) {
                executor.submit(() -> {
                    try {
                        startLatch.await();
                        HdfsWriteResult result = payloadWriter.write(sharedPayload);
                        if (result.isNewWrite()) {
                            newWriteCount.incrementAndGet();
                        } else {
                            duplicateCount.incrementAndGet();
                        }
                    } catch (Exception e) {
                        errorCount.incrementAndGet();
                    } finally {
                        doneLatch.countDown();
                    }
                });
            }

            startLatch.countDown();
            doneLatch.await();
            executor.shutdown();

            int totalProcessed = newWriteCount.get() + duplicateCount.get() + errorCount.get();
            assertThat(totalProcessed).isEqualTo(threadCount);
            assertThat(newWriteCount.get()).isGreaterThanOrEqualTo(1);
        }
    }

    @Nested
    @DisplayName("File Content Verification Tests")
    class FileContentVerificationTests {

        @Test
        @DisplayName("should write valid JSON content")
        void shouldWriteValidJsonContent() throws IOException {
            EnrichedPayload payload = payloadGenerator.generateEnrichedPayload();

            HdfsWriteResult result = payloadWriter.write(payload);

            String content = hdfsOperations.readFile(result.getHdfsPath());
            assertThat(content).contains("messageId");
            assertThat(content).contains("transactionId");
            assertThat(content).contains("enrichmentData");
        }

        @Test
        @DisplayName("should preserve enrichment data in written file")
        void shouldPreserveEnrichmentData() throws IOException {
            EnrichedPayload payload = payloadGenerator.generateEnrichedPayload();

            HdfsWriteResult result = payloadWriter.write(payload);

            String content = hdfsOperations.readFile(result.getHdfsPath());
            assertThat(content).contains("marketingPlanId");
            assertThat(content).contains("campaignId");
        }

        @Test
        @DisplayName("should report correct bytes written")
        void shouldReportCorrectBytesWritten() throws IOException {
            EnrichedPayload payload = payloadGenerator.generateEnrichedPayload();

            HdfsWriteResult result = payloadWriter.write(payload);

            Path filePath = tempDir.resolve(result.getHdfsPath().substring(1));
            long actualSize = Files.size(filePath);
            assertThat(result.getBytesWritten()).isEqualTo(actualSize);
        }
    }

    @Nested
    @DisplayName("Path Generation Tests")
    class PathGenerationTests {

        @Test
        @DisplayName("should generate path with event type")
        void shouldGeneratePathWithEventType() {
            EnrichedPayload payload = payloadGenerator.generateEnrichedPayload();

            HdfsWriteResult result = payloadWriter.write(payload);

            assertThat(result.getHdfsPath()).contains(payload.getEventType().toLowerCase());
        }

        @Test
        @DisplayName("should generate path with date hierarchy")
        void shouldGeneratePathWithDateHierarchy() {
            EnrichedPayload payload = payloadGenerator.generateEnrichedPayload();

            HdfsWriteResult result = payloadWriter.write(payload);

            assertThat(result.getHdfsPath()).matches(".*\\d{4}/\\d{2}/\\d{2}.*");
        }

        @Test
        @DisplayName("should include transaction and message ID in filename")
        void shouldIncludeIdsInFilename() {
            EnrichedPayload payload = payloadGenerator.generateEnrichedPayload();

            HdfsWriteResult result = payloadWriter.write(payload);

            assertThat(result.getHdfsPath()).contains(payload.getTransactionId());
            assertThat(result.getHdfsPath()).contains(payload.getMessageId());
        }
    }
}
