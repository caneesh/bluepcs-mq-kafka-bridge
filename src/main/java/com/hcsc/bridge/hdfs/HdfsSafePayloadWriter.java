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

        try {
            if (hdfsFileOperations.exists(targetPath)) {
                logger.info("File already exists for message {}: {}", messageId, targetPath);
                String existingChecksum = hdfsFileOperations.getFileChecksum(targetPath);
                return HdfsWriteResult.alreadyExists(targetPath, existingChecksum);
            }

            ensureParentDirectoryExists(targetPath);

            byte[] contentBytes = content.getBytes(StandardCharsets.UTF_8);
            String checksum = calculateChecksum(contentBytes);

            writeToTempFile(tempPath, contentBytes, messageId);

            boolean renamed = hdfsFileOperations.rename(tempPath, targetPath);
            if (!renamed) {
                throw new HdfsWriteException("Failed to rename temp file to target", targetPath, messageId);
            }

            String finalChecksum = hdfsFileOperations.getFileChecksum(targetPath);
            if (!checksum.equals(finalChecksum)) {
                throw new HdfsWriteException(
                        "Checksum mismatch after write: expected " + checksum + " but got " + finalChecksum,
                        targetPath, messageId);
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
        try {
            if (hdfsFileOperations.exists(tempPath)) {
                hdfsFileOperations.delete(tempPath);
                logger.debug("Cleaned up temp file: {}", tempPath);
            }
        } catch (IOException e) {
            logger.warn("Failed to cleanup temp file: {}", tempPath, e);
        }
    }
}
