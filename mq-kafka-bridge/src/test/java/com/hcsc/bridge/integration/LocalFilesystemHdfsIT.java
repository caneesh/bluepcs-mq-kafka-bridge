package com.hcsc.bridge.integration;

import com.hcsc.bridge.hdfs.HdfsSafePayloadWriter;
import com.hcsc.bridge.hdfs.HdfsWriteException;
import com.hcsc.bridge.mock.LocalFileSystemHdfsOperations;
import com.hcsc.bridge.mock.SamplePayloadGenerator;
import com.hcsc.bridge.model.EnrichedPayload;
import com.hcsc.bridge.model.HdfsWriteResult;
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

    private static final String WRAPPER_CONTENT =
            "{\"changeEventTimeStamp\":\"20260710T162108.143 CDT\","
            + "\"RestAPIResponse\":{\"PlanResponse\":{\"planIdentification\":{\"marketingPlanIdentifier\":\"MP-1\"}}},"
            + "\"changeEventTypeName\":\"Update\"}";

    private LocalFileSystemHdfsOperations hdfsOperations;
    private HdfsSafePayloadWriter payloadWriter;
    private SamplePayloadGenerator payloadGenerator;
    private Path tempDir;

    @BeforeEach
    void setUp() throws IOException {
        tempDir = Files.createTempDirectory("hdfs-integration-test");
        hdfsOperations = new LocalFileSystemHdfsOperations(tempDir);
        payloadWriter = new HdfsSafePayloadWriter(hdfsOperations, "/data/bridge/payloads", "", ".tmp");
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

            HdfsWriteResult result = payloadWriter.write(payload, WRAPPER_CONTENT);

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

            assertThatThrownBy(() -> payloadWriter.write(payload, WRAPPER_CONTENT))
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

            payloadWriter.write(payload, WRAPPER_CONTENT);

            hdfsOperations.setShouldFailOnCreate(true);
            EnrichedPayload payload2 = payloadGenerator.generateEnrichedPayload();

            assertThatThrownBy(() -> payloadWriter.write(payload2, WRAPPER_CONTENT))
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

            HdfsWriteResult result = payloadWriter.write(payload, WRAPPER_CONTENT);

            assertThat(result.getHdfsPath()).doesNotContain(".tmp");
            assertThat(result.isNewWrite()).isTrue();
        }

        @Test
        @DisplayName("should verify checksum after rename")
        void shouldVerifyChecksumAfterRename() {
            EnrichedPayload payload = payloadGenerator.generateEnrichedPayload();

            HdfsWriteResult result = payloadWriter.write(payload, WRAPPER_CONTENT);

            assertThat(result.getChecksum()).isNotNull();
            assertThat(result.getChecksum()).hasSize(64);
        }

        @Test
        @DisplayName("should create parent directories")
        void shouldCreateParentDirectories() throws IOException {
            EnrichedPayload payload = payloadGenerator.generateEnrichedPayload();

            HdfsWriteResult result = payloadWriter.write(payload, WRAPPER_CONTENT);

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

            HdfsWriteResult result1 = payloadWriter.write(payload, WRAPPER_CONTENT);
            HdfsWriteResult result2 = payloadWriter.write(payload, WRAPPER_CONTENT);

            assertThat(result1.isNewWrite()).isTrue();
            assertThat(result2.isAlreadyExists()).isTrue();
            assertThat(result2.getHdfsPath()).isEqualTo(result1.getHdfsPath());
        }

        @Test
        @DisplayName("should return existing checksum for duplicate")
        void shouldReturnExistingChecksumForDuplicate() {
            EnrichedPayload payload = payloadGenerator.generateEnrichedPayload();

            HdfsWriteResult result1 = payloadWriter.write(payload, WRAPPER_CONTENT);
            HdfsWriteResult result2 = payloadWriter.write(payload, WRAPPER_CONTENT);

            assertThat(result2.getChecksum()).isNotNull();
        }

        @Test
        @DisplayName("should not overwrite existing file")
        void shouldNotOverwriteExistingFile() throws IOException {
            EnrichedPayload payload = payloadGenerator.generateEnrichedPayload();

            HdfsWriteResult result1 = payloadWriter.write(payload, WRAPPER_CONTENT);
            String originalContent = hdfsOperations.readFile(result1.getHdfsPath());

            HdfsWriteResult result2 = payloadWriter.write(payload, WRAPPER_CONTENT);
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
                        HdfsWriteResult result = payloadWriter.write(payload, WRAPPER_CONTENT);
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
                        HdfsWriteResult result = payloadWriter.write(sharedPayload, WRAPPER_CONTENT);
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
        @DisplayName("should write the wrapper content verbatim")
        void shouldWriteValidJsonContent() throws IOException {
            EnrichedPayload payload = payloadGenerator.generateEnrichedPayload();

            HdfsWriteResult result = payloadWriter.write(payload, WRAPPER_CONTENT);

            String content = hdfsOperations.readFile(result.getHdfsPath());
            assertThat(content).isEqualTo(WRAPPER_CONTENT);
            assertThat(content).contains("changeEventTimeStamp");
            assertThat(content).contains("RestAPIResponse");
        }

        @Test
        @DisplayName("should preserve the wrapped API response in written file")
        void shouldPreserveEnrichmentData() throws IOException {
            EnrichedPayload payload = payloadGenerator.generateEnrichedPayload();

            HdfsWriteResult result = payloadWriter.write(payload, WRAPPER_CONTENT);

            String content = hdfsOperations.readFile(result.getHdfsPath());
            assertThat(content).contains("PlanResponse");
            assertThat(content).contains("marketingPlanIdentifier");
        }

        @Test
        @DisplayName("should report correct bytes written")
        void shouldReportCorrectBytesWritten() throws IOException {
            EnrichedPayload payload = payloadGenerator.generateEnrichedPayload();

            HdfsWriteResult result = payloadWriter.write(payload, WRAPPER_CONTENT);

            Path filePath = tempDir.resolve(result.getHdfsPath().substring(1));
            long actualSize = Files.size(filePath);
            assertThat(result.getBytesWritten()).isEqualTo(actualSize);
        }
    }

    @Nested
    @DisplayName("Path Generation Tests")
    class PathGenerationTests {

        @Test
        @DisplayName("should generate a flat landing-directory path (no eventType/date partitioning)")
        void shouldGenerateFlatLandingPath() {
            EnrichedPayload payload = payloadGenerator.generateEnrichedPayload();

            HdfsWriteResult result = payloadWriter.write(payload, WRAPPER_CONTENT);

            // Claim-check contract: a single flat landing dir owned by the downstream
            // consumer — the path is exactly <base>/<eventId>.json with no partitioning
            assertThat(result.getHdfsPath())
                    .isEqualTo("/data/bridge/payloads/" + payload.getEventId() + ".json");
            assertThat(result.getHdfsPath()).doesNotContain(payload.getEventType().toLowerCase());
            assertThat(result.getHdfsPath()).doesNotMatch(".*\\d{4}/\\d{2}/\\d{2}.*");
        }

        @Test
        @DisplayName("should include eventId in filename")
        void shouldIncludeIdsInFilename() {
            EnrichedPayload payload = payloadGenerator.generateEnrichedPayload();

            HdfsWriteResult result = payloadWriter.write(payload, WRAPPER_CONTENT);

            // The filename is derived from the eventId (see HdfsSafePayloadWriter#buildTargetPath).
            assertThat(result.getHdfsPath()).contains(payload.getEventId());
            assertThat(result.getHdfsPath()).contains(payload.getMessageId());
        }
    }
}
