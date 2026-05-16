package com.enterprise.bridge.kafka;

public class KafkaPublishException extends RuntimeException {

    private final String messageId;
    private final String topic;

    public KafkaPublishException(String message, String messageId, String topic) {
        super(message);
        this.messageId = messageId;
        this.topic = topic;
    }

    public KafkaPublishException(String message, String messageId, String topic, Throwable cause) {
        super(message, cause);
        this.messageId = messageId;
        this.topic = topic;
    }

    public String getMessageId() {
        return messageId;
    }

    public String getTopic() {
        return topic;
    }
}
