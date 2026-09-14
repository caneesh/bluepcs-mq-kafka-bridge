package com.hcsc.bridge.parser;

public class MessageParseException extends RuntimeException {

    private final String messageId;
    private final String rawPayload;

    public MessageParseException(String message, String messageId, String rawPayload) {
        super(message);
        this.messageId = messageId;
        this.rawPayload = rawPayload;
    }

    public MessageParseException(String message, String messageId, String rawPayload, Throwable cause) {
        super(message, cause);
        this.messageId = messageId;
        this.rawPayload = rawPayload;
    }

    public String getMessageId() {
        return messageId;
    }

    public String getRawPayload() {
        return rawPayload;
    }
}
