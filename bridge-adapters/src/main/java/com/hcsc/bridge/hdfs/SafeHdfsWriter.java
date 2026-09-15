package com.hcsc.bridge.hdfs;

import com.hcsc.bridge.core.DigestUtil;
import com.hcsc.bridge.model.HdfsWriteResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * Path-agnostic "safe write" to HDFS: temp file → checksum-verify → atomic rename →
 * verify an already-existing target. Shared by every bridge application; the layout
 * decisions (which directory, which extension, quarantine location) belong to the
 * caller, which passes the final target path.
 *
 * <p>Idempotency contract: the target path must be a pure function of the message's
 * deterministic eventId. A redelivery then either finds the file (accepted only if the
 * bytes match) or writes it; two attempts can never both succeed with different content.
 */
@Component
public class SafeHdfsWriter {

    private static final Logger logger = LoggerFactory.getLogger(SafeHdfsWriter.class);

    private final HdfsFileOperations hdfsFileOperations;
    private final String tempSuffix;

    public SafeHdfsWriter(HdfsFileOperations hdfsFileOperations,
                          @Value("${bridge.hdfs.temp-suffix:.tmp}") String tempSuffix) {
        this.hdfsFileOperations = hdfsFileOperations;
        this.tempSuffix = tempSuffix;
    }

    /**
     * Writes the UTF-8 encoding of {@code content} to {@code targetPath}, creating the
     * parent directory if needed. {@code messageId} is used only for logging/errors.
     *
     * @throws HdfsWriteException on any write, checksum or rename failure, and when the
     *         target already exists with different bytes (refused, never overwritten)
     */
    public HdfsWriteResult write(String targetPath, String content, String messageId) {
        String tempPath = buildTempPath(targetPath);

        logger.debug("Writing payload {} to HDFS: {}", messageId, targetPath);

        byte[] contentBytes = content.getBytes(StandardCharsets.UTF_8);
        String checksum = DigestUtil.sha256Hex(contentBytes);

        try {
            if (hdfsFileOperations.exists(targetPath)) {
                return verifyExistingTarget(targetPath, checksum, messageId);
            }

            ensureParentDirectoryExists(targetPath);

            writeToTempFile(tempPath, contentBytes, messageId);

            // Verify the checksum on the temp file BEFORE the rename: once a bad file is
            // renamed into place it would satisfy the exists() idempotency check on redelivery
            // and the corruption would become permanent and acked.
            String writtenChecksum = hdfsFileOperations.getFileChecksum(tempPath);
            if (!checksum.equals(writtenChecksum)) {
                throw new HdfsWriteException(
                        "Checksum mismatch after write: expected " + checksum + " but got " + writtenChecksum,
                        targetPath, messageId);
            }

            boolean renamed = hdfsFileOperations.rename(tempPath, targetPath);
            if (!renamed) {
                // Concurrency: listener concurrency is 1 per JVM, but a second bridge
                // instance (blue/green overlap, double-start) can process the same event.
                // Losing the rename race to a peer that created the target is idempotent
                // success IF the peer wrote the same bytes — verify, don't assume.
                if (hdfsFileOperations.exists(targetPath)) {
                    cleanupTempFile(tempPath);
                    return verifyExistingTarget(targetPath, checksum, messageId);
                }
                // rename() returning false covers several distinct causes — probe so the
                // failure is diagnosable from the log alone
                throw new HdfsWriteException("Failed to rename temp file to target ("
                        + describeRenameFailure(tempPath, targetPath) + ")", targetPath, messageId);
            }

            // DEBUG: the orchestrator's terminal summary already carries the path at
            // INFO; this line adds only the byte count on the happy path
            logger.debug("Successfully wrote payload {} to HDFS: {} ({} bytes)",
                    messageId, targetPath, contentBytes.length);

            return HdfsWriteResult.success(targetPath, checksum, contentBytes.length);

        } catch (HdfsWriteException e) {
            cleanupTempFile(tempPath);
            throw e;
        } catch (IOException e) {
            cleanupTempFile(tempPath);
            throw new HdfsWriteException("Failed to write payload to HDFS", targetPath, messageId, e);
        } catch (RuntimeException e) {
            // Hadoop client code can surface unchecked exceptions (wrapped
            // AccessControlException, IPC/Kerberos failures) between create and rename —
            // without this the temp file would be orphaned in the landing directory.
            cleanupTempFile(tempPath);
            throw e;
        }
    }

    /**
     * An existing target is only an idempotent redelivery if it holds the SAME bytes this
     * message would write. Accepting it blindly would let a stale, truncated, or foreign
     * file under the deterministic eventId name be advertised downstream as valid — and
     * permanently, because the message then gets acked. On mismatch, refuse without
     * overwriting: the message stays on the queue and the file needs manual review.
     *
     * <p>NOTE: this assumes the bytes are stable across redeliveries. If the content
     * carries volatile fields (response timestamps, request ids), a caller must avoid
     * re-generating it on redelivery — e.g. by checking {@link HdfsFileOperations#exists}
     * for the target before calling the service that produces the content.
     */
    private HdfsWriteResult verifyExistingTarget(String targetPath, String expectedChecksum,
                                                 String messageId) throws IOException {
        String existingChecksum = hdfsFileOperations.getFileChecksum(targetPath);
        if (!expectedChecksum.equals(existingChecksum)) {
            throw new HdfsWriteException(
                    "Existing file checksum mismatch for message " + messageId
                            + ": expected " + expectedChecksum + " but found " + existingChecksum
                            + "; refusing to accept or overwrite — manual review required",
                    targetPath, messageId);
        }
        logger.info("File already exists with matching checksum for message {}: {}",
                messageId, targetPath);
        return HdfsWriteResult.alreadyExists(targetPath, existingChecksum);
    }

    /** Best-effort diagnosis of a rename() that returned false. */
    private String describeRenameFailure(String tempPath, String targetPath) {
        try {
            return "source exists=" + hdfsFileOperations.exists(tempPath)
                    + ", target exists=" + hdfsFileOperations.exists(targetPath);
        } catch (Exception probeFailure) {
            return "probe failed: " + probeFailure.getMessage();
        }
    }

    /**
     * Unique per attempt so two bridge instances (or a retry racing a stale attempt)
     * can never write to or delete each other's temp file. The random token sits
     * BEFORE the file extension so the name still ends in "{ext}{tempSuffix}" and the
     * orphan sweeps can match on it ({@code *.json.tmp}, {@code *.xml.tmp}). A target
     * without an extension gets "{target}.{token}{tempSuffix}".
     */
    String buildTempPath(String targetPath) {
        String token = UUID.randomUUID().toString();
        int lastSlash = targetPath.lastIndexOf('/');
        int lastDot = targetPath.lastIndexOf('.');
        if (lastDot > lastSlash + 1) {
            return targetPath.substring(0, lastDot) + "." + token + targetPath.substring(lastDot) + tempSuffix;
        }
        return targetPath + "." + token + tempSuffix;
    }

    private void ensureParentDirectoryExists(String filePath) throws IOException {
        int lastSlash = filePath.lastIndexOf('/');
        if (lastSlash > 0) {
            String parentPath = filePath.substring(0, lastSlash);
            hdfsFileOperations.mkdirs(parentPath);
        }
    }

    private void writeToTempFile(String tempPath, byte[] content, String messageId) throws IOException {
        try (OutputStream out = hdfsFileOperations.create(tempPath)) {
            out.write(content);
            out.flush();
        } catch (IOException e) {
            throw new HdfsWriteException("Failed to write temp file", tempPath, messageId, e);
        }
    }

    private void cleanupTempFile(String tempPath) {
        // Catches Exception, not just IOException: an unchecked failure here must never
        // replace the original write exception the caller is about to throw.
        try {
            if (hdfsFileOperations.exists(tempPath)) {
                hdfsFileOperations.delete(tempPath);
                logger.debug("Cleaned up temp file: {}", tempPath);
            }
        } catch (Exception e) {
            logger.warn("Failed to cleanup temp file: {}", tempPath, e);
        }
    }
}
