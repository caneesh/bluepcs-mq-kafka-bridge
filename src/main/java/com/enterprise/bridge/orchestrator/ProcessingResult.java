package com.enterprise.bridge.orchestrator;

import java.util.Objects;

public final class ProcessingResult {

    private final String eventId;
    private final Status status;
    private final String hdfsPath;
    private final String kafkaOffset;
    private final String errorCode;
    private final String errorMessage;

    private ProcessingResult(String eventId, Status status, String hdfsPath,
                             String kafkaOffset, String errorCode, String errorMessage) {
        this.eventId = Objects.requireNonNull(eventId, "eventId must not be null");
        this.status = Objects.requireNonNull(status, "status must not be null");
        this.hdfsPath = hdfsPath;
        this.kafkaOffset = kafkaOffset;
        this.errorCode = errorCode;
        this.errorMessage = errorMessage;
    }

    public static ProcessingResult success(String eventId, String hdfsPath, String kafkaOffset) {
        return new ProcessingResult(eventId, Status.SUCCESS, hdfsPath, kafkaOffset, null, null);
    }

    public static ProcessingResult failure(String eventId, String errorCode, String errorMessage) {
        return new ProcessingResult(eventId, Status.FAILURE, null, null, errorCode, errorMessage);
    }

    public String getEventId() {
        return eventId;
    }

    public Status getStatus() {
        return status;
    }

    public String getHdfsPath() {
        return hdfsPath;
    }

    public String getKafkaOffset() {
        return kafkaOffset;
    }

    public String getErrorCode() {
        return errorCode;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public boolean isSuccessful() {
        return status == Status.SUCCESS;
    }

    public boolean isFailed() {
        return status == Status.FAILURE;
    }

    @Override
    public String toString() {
        return "ProcessingResult{" +
                "eventId='" + eventId + '\'' +
                ", status=" + status +
                ", hdfsPath='" + hdfsPath + '\'' +
                ", kafkaOffset='" + kafkaOffset + '\'' +
                ", errorCode='" + errorCode + '\'' +
                '}';
    }

    public enum Status {
        SUCCESS,
        FAILURE
    }
}
