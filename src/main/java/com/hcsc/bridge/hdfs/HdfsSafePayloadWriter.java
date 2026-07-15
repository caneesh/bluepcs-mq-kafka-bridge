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
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

@Component
public class HdfsSafePayloadWriter {

    private static final Logger logger = LoggerFactory.getLogger(HdfsSafePayloadWriter.class);
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy/MM/dd");

    private final HdfsFileOperations hdfsFileOperations;
    private final String basePath;
    private final String tempSuffix;

    public HdfsSafePayloadWriter(
            HdfsFileOperations hdfsFileOperations,
            @Value("${bridge.hdfs.base-path:/data/bridge/payloads}") String basePath,
            @Value("${bridge.hdfs.temp-suffix:.tmp}") String tempSuffix) {
        this.hdfsFileOperations = hdfsFileOperations;
        this.basePath = basePath;
        this.tempSuffix = tempSuffix;
    }

    /**
     * Writes the given wrapper {@code content} to HDFS. The {@code payload} is used only
     * for path building (eventType/eventId) and for the messageId used in logging/errors;
     * the bytes written are the UTF-8 encoding of {@code content}.
     */
    public HdfsWriteResult write(EnrichedPayload payload, String content) {
        String targetPath = buildTargetPath(payload);
        String tempPath = targetPath + tempSuffix;
        String messageId = payload.getMessageId();

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
        String datePath = LocalDate.now().format(DATE_FORMATTER);
        String eventType = payload.getEventType().toLowerCase();
        String fileName = payload.getEventId() + ".json";
        return basePath + "/" + eventType + "/" + datePath + "/" + fileName;
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
