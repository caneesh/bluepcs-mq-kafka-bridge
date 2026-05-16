package com.enterprise.bridge.orchestrator;

import java.util.Objects;

public final class ProcessingResult {

    private final String messageId;
    private final Status status;
    private final String hdfsPath;
    private final String kafkaOffset;
    private final String errorCode;
    private final String errorMessage;

    private ProcessingResult(String messageId, Status status, String hdfsPath,
                             String kafkaOffset, String errorCode, String errorMessage) {
        this.messageId = Objects.requireNonNull(messageId, "messageId must not be null");
        this.status = Objects.requireNonNull(status, "status must not be null");
        this.hdfsPath = hdfsPath;
        this.kafkaOffset = kafkaOffset;
        this.errorCode = errorCode;
        this.errorMessage = errorMessage;
    }

    public static ProcessingResult success(String messageId, String hdfsPath, String kafkaOffset) {
        return new ProcessingResult(messageId, Status.SUCCESS, hdfsPath, kafkaOffset, null, null);
    }

    public static ProcessingResult failure(String messageId, String errorCode, String errorMessage) {
        return new ProcessingResult(messageId, Status.FAILURE, null, null, errorCode, errorMessage);
    }

    public static ProcessingResult duplicate(String messageId) {
        return new ProcessingResult(messageId, Status.DUPLICATE, null, null, null, null);
    }

    public String getMessageId() {
        return messageId;
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

    public boolean isDuplicate() {
        return status == Status.DUPLICATE;
    }

    @Override
    public String toString() {
        return "ProcessingResult{" +
                "messageId='" + messageId + '\'' +
                ", status=" + status +
                ", hdfsPath='" + hdfsPath + '\'' +
                ", kafkaOffset='" + kafkaOffset + '\'' +
                ", errorCode='" + errorCode + '\'' +
                '}';
    }

    public enum Status {
        SUCCESS,
        FAILURE,
        DUPLICATE
    }
}
