package com.hcsc.bridge.hdfs;

import com.hcsc.bridge.model.EnrichedPayload;
import com.hcsc.bridge.model.HdfsWriteResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * PMM+ landing-directory layout on top of the generic {@link SafeHdfsWriter}:
 * {@code <base-path>/<eventId>.json} for enriched payloads and
 * {@code <error-path>/<eventId>.json} for quarantined raw messages.
 */
@Component
public class HdfsSafePayloadWriter {

    private final SafeHdfsWriter safeWriter;
    private final String basePath;
    private final String errorPath;

    @Autowired
    public HdfsSafePayloadWriter(
            HdfsFileOperations hdfsFileOperations,
            @Value("${bridge.hdfs.base-path:/data/bridge/payloads}") String basePath,
            @Value("${bridge.hdfs.error-path:}") String errorPath,
            @Value("${bridge.hdfs.temp-suffix:.tmp}") String tempSuffix) {
        this(new SafeHdfsWriter(hdfsFileOperations, tempSuffix), basePath, errorPath);
    }

    public HdfsSafePayloadWriter(SafeHdfsWriter safeWriter, String basePath, String errorPath) {
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
