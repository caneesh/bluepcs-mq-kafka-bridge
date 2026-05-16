package com.enterprise.bridge.hdfs;

public class HdfsWriteException extends RuntimeException {

    private final String targetPath;
    private final String messageId;

    public HdfsWriteException(String message, String targetPath, String messageId) {
        super(message);
        this.targetPath = targetPath;
        this.messageId = messageId;
    }

    public HdfsWriteException(String message, String targetPath, String messageId, Throwable cause) {
        super(message, cause);
        this.targetPath = targetPath;
        this.messageId = messageId;
    }

    public String getTargetPath() {
        return targetPath;
    }

    public String getMessageId() {
        return messageId;
    }
}
