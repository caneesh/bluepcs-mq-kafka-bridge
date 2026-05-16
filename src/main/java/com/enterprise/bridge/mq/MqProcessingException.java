package com.enterprise.bridge.mq;

public class MqProcessingException extends RuntimeException {

    private final String messageId;
    private final String errorDetail;

    public MqProcessingException(String message, String messageId, String errorDetail) {
        super(message);
        this.messageId = messageId;
        this.errorDetail = errorDetail;
    }

    public MqProcessingException(String message, String messageId, String errorDetail, Throwable cause) {
        super(message, cause);
        this.messageId = messageId;
        this.errorDetail = errorDetail;
    }

    public String getMessageId() {
        return messageId;
    }

    public String getErrorDetail() {
        return errorDetail;
    }
}
