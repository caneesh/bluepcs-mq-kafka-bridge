package com.hcsc.bridge.hdfs;

import com.hcsc.bridge.model.EnrichedPayload;
import com.hcsc.bridge.model.HdfsWriteResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Objects;

/**
 * PMM+ landing-directory layout on top of the generic {@link SafeHdfsWriter}:
 * {@code <base-path>/<eventId>.json} for enriched payloads,
 * {@code <error-path>/<eventId>.json} for quarantined raw messages, and
 * {@code <archive-path>/<eventId>.json} for files the consumer (or the retention sweep)
 * has moved out of the landing directory.
 */
@Component
public class HdfsSafePayloadWriter {

    private final HdfsFileOperations hdfsFileOperations;
    private final SafeHdfsWriter safeWriter;
    private final String basePath;
    private final String errorPath;
    private final String archivePath;

    /** Test/legacy constructor: archive path defaults to {@code <base-path>/archive}. */
    public HdfsSafePayloadWriter(
            HdfsFileOperations hdfsFileOperations,
            String basePath,
            String errorPath,
            String tempSuffix) {
        this(hdfsFileOperations, new SafeHdfsWriter(hdfsFileOperations, tempSuffix), basePath, errorPath, "");
    }

    @Autowired
    public HdfsSafePayloadWriter(
            HdfsFileOperations hdfsFileOperations,
            @Value("${bridge.hdfs.base-path:/data/bridge/payloads}") String basePath,
            @Value("${bridge.hdfs.error-path:}") String errorPath,
            @Value("${bridge.hdfs.temp-suffix:.tmp}") String tempSuffix,
            @Value("${bridge.hdfs.archive-path:}") String archivePath) {
        this(hdfsFileOperations, new SafeHdfsWriter(hdfsFileOperations, tempSuffix), basePath, errorPath, archivePath);
    }

    public HdfsSafePayloadWriter(HdfsFileOperations hdfsFileOperations, SafeHdfsWriter safeWriter,
                                 String basePath, String errorPath, String archivePath) {
        this.hdfsFileOperations = hdfsFileOperations;
        this.safeWriter = safeWriter;
        // Tolerate a trailing slash on the configured base path — the advertised
        // hdfsPath must stay clean (no "//") for consumers comparing paths
        this.basePath = basePath.replaceAll("/+$", "");
        // Quarantine directory for unparseable payloads. Defaults to a sibling of the
        // landing directory (same pattern as archive) so it never pollutes the flat
        // landing dir that the downstream consumer sweeps.
        this.errorPath = (errorPath == null || errorPath.trim().isEmpty())
                ? this.basePath + "/errors"
                : errorPath.replaceAll("/+$", "");
        // Where the consumer moves processed files (and where hdfs-landing-cleanup.sh
        // sweeps never-processed ones); the same default as the sweep.
        this.archivePath = (archivePath == null || archivePath.trim().isEmpty())
                ? this.basePath + "/archive"
                : archivePath.replaceAll("/+$", "");
    }

    /**
     * The file this message's payload landed in on an earlier delivery, if any: first the
     * landing directory, then the archive. Redelivery MUST resume from it instead of
     * calling the enrichment API again — a second call can return different bytes (a
     * newer plan version, a timestamp), which the writer would then refuse against the
     * existing file and wedge the message; and once the consumer has archived the file,
     * re-landing a different payload under the same name would invalidate the checksum
     * carried by the notification it already processed.
     *
     * @return null when nothing has landed for this eventId
     * @throws HdfsWriteException when HDFS cannot be consulted (retryable: no ack)
     */
    @Nullable
    public LandedPayload findLanded(String eventId) {
        String landing = basePath + "/" + eventId + ".json";
        String archived = archivePath + "/" + eventId + ".json";
        try {
            if (hdfsFileOperations.exists(landing)) {
                return new LandedPayload(landing, hdfsFileOperations.readUtf8(landing),
                        hdfsFileOperations.getFileChecksum(landing), false);
            }
            if (hdfsFileOperations.exists(archived)) {
                return new LandedPayload(archived, hdfsFileOperations.readUtf8(archived),
                        hdfsFileOperations.getFileChecksum(archived), true);
            }
            return null;
        } catch (IOException e) {
            throw new HdfsWriteException("Failed to look up a previously landed payload", landing, eventId, e);
        }
    }

    /** A payload found on HDFS from an earlier delivery of the same message. */
    public static final class LandedPayload {
        private final String path;
        private final String content;
        private final String checksum;
        private final boolean archived;

        public LandedPayload(String path, @Nullable String content, String checksum, boolean archived) {
            this.path = Objects.requireNonNull(path, "path");
            this.content = content;
            this.checksum = checksum;
            this.archived = archived;
        }

        public String getPath() {
            return path;
        }

        /** The wrapper JSON as landed. */
        public String getContent() {
            return content;
        }

        public String getChecksum() {
            return checksum;
        }

        /** True when the file was found in the archive rather than the landing directory. */
        public boolean isArchived() {
            return archived;
        }
    }

    /**
     * Writes the given wrapper {@code content} to HDFS. The {@code payload} is used only
     * for path building (eventId) and for the messageId used in logging/errors;
     * the bytes written are the UTF-8 encoding of {@code content}.
     */
    public HdfsWriteResult write(EnrichedPayload payload, String content) {
        return safeWriter.write(buildTargetPath(payload), content, payload.getMessageId());
    }

    /**
     * Preserves the raw payload of an unparseable message in the quarantine (error)
     * directory: {@code <error-path>/<eventId>.json}. Uses the same temp-write / rename /
     * checksum-verify sequence as the landing-directory write, and is idempotent by
     * eventId so a redelivered message quarantines to the same file.
     */
    public HdfsWriteResult writeQuarantine(String eventId, String rawPayload, String messageId) {
        return safeWriter.write(errorPath + "/" + eventId + ".json",
                rawPayload != null ? rawPayload : "", messageId);
    }

    private String buildTargetPath(EnrichedPayload payload) {
        // Single flat landing directory: the consumer owns the file lifecycle and
        // moves processed files to archive/error locations. Partitioning by
        // message-derived values (eventType) proved unreliable, and the eventId
        // filename alone keeps redelivered messages idempotent.
        return basePath + "/" + payload.getEventId() + ".json";
    }
}
