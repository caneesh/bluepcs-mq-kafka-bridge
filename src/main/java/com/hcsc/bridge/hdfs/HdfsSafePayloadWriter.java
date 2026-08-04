package com.hcsc.bridge.hdfs;

import com.hcsc.bridge.model.EnrichedPayload;
import com.hcsc.bridge.model.HdfsWriteResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

@Component
public class HdfsSafePayloadWriter {

    private static final Logger logger = LoggerFactory.getLogger(HdfsSafePayloadWriter.class);

    private final HdfsFileOperations hdfsFileOperations;
    private final String basePath;
    private final String errorPath;
    private final String tempSuffix;

    public HdfsSafePayloadWriter(
            HdfsFileOperations hdfsFileOperations,
            @Value("${bridge.hdfs.base-path:/data/bridge/payloads}") String basePath,
            @Value("${bridge.hdfs.error-path:}") String errorPath,
            @Value("${bridge.hdfs.temp-suffix:.tmp}") String tempSuffix) {
        this.hdfsFileOperations = hdfsFileOperations;
        // Tolerate a trailing slash on the configured base path — the advertised
        // hdfsPath must stay clean (no "//") for consumers comparing paths
        this.basePath = basePath.replaceAll("/+$", "");
        // Quarantine directory for unparseable payloads. Defaults to a sibling of the
        // landing directory (same pattern as archive) so it never pollutes the flat
        // landing dir that the downstream consumer sweeps.
        this.errorPath = (errorPath == null || errorPath.trim().isEmpty())
                ? this.basePath + "/errors"
                : errorPath.replaceAll("/+$", "");
        this.tempSuffix = tempSuffix;
    }

    /**
     * Writes the given wrapper {@code content} to HDFS. The {@code payload} is used only
     * for path building (eventId) and for the messageId used in logging/errors;
     * the bytes written are the UTF-8 encoding of {@code content}.
     */
    public HdfsWriteResult write(EnrichedPayload payload, String content) {
        return safeWrite(buildTargetPath(payload), content, payload.getMessageId());
    }

    /**
     * Preserves the raw payload of an unparseable message in the quarantine (error)
     * directory: {@code <error-path>/<eventId>.json}. Uses the same temp-write / rename /
     * checksum-verify sequence as the landing-directory write, and is idempotent by
     * eventId so a redelivered message quarantines to the same file.
     */
    public HdfsWriteResult writeQuarantine(String eventId, String rawPayload, String messageId) {
        return safeWrite(errorPath + "/" + eventId + ".json",
                rawPayload != null ? rawPayload : "", messageId);
    }

    private HdfsWriteResult safeWrite(String targetPath, String content, String messageId) {
        String tempPath = targetPath + tempSuffix;

        logger.debug("Writing payload {} to HDFS: {}", messageId, targetPath);

        byte[] contentBytes = content.getBytes(StandardCharsets.UTF_8);
        String checksum = calculateChecksum(contentBytes);

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
                // rename() returning false covers several distinct causes — probe so the
                // failure is diagnosable from the log alone
                throw new HdfsWriteException("Failed to rename temp file to target ("
                        + describeRenameFailure(tempPath, targetPath) + ")", targetPath, messageId);
            }

            logger.info("Successfully wrote payload {} to HDFS: {} ({} bytes)",
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
     * file under the deterministic eventId name be advertised to Kafka as valid — and
     * permanently, because the message then gets acked. On mismatch, refuse without
     * overwriting: the message stays on the queue and the file needs manual review.
     *
     * <p>NOTE: this assumes the wrapper bytes are stable across redeliveries, i.e. the
     * enrichment API returns identical content for the same plan/effective-date within
     * the redelivery window. If the API response carries volatile fields (timestamps,
     * request ids), redelivery after a partial failure would mismatch here and wedge the
     * message — verify response stability in UAT before relying on redelivery.
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

    private String buildTargetPath(EnrichedPayload payload) {
        // Single flat landing directory: the consumer owns the file lifecycle and
        // moves processed files to archive/error locations. Partitioning by
        // message-derived values (eventType) proved unreliable, and the eventId
        // filename alone keeps redelivered messages idempotent.
        return basePath + "/" + payload.getEventId() + ".json";
    }

    private void ensureParentDirectoryExists(String filePath) throws IOException {
        int lastSlash = filePath.lastIndexOf('/');
        if (lastSlash > 0) {
            String parentPath = filePath.substring(0, lastSlash);
            hdfsFileOperations.mkdirs(parentPath);
        }
    }

    private String calculateChecksum(byte[] content) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(content);
            return bytesToHex(hash);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-256 algorithm not available", e);
        }
    }

    private String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
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
