package com.hcsc.bridge.pmm.xml;

/**
 * The MQ message cannot yield the two request values: malformed XML, a rejected
 * DOCTYPE, an XPath with no match, or a blank value. Always PERMANENT — redelivering
 * the same bytes can never succeed — so the orchestrator quarantines and acks.
 */
public class PmmXmlException extends RuntimeException {

    private final String messageId;

    public PmmXmlException(String message, String messageId) {
        super(message);
        this.messageId = messageId;
    }

    public PmmXmlException(String message, String messageId, Throwable cause) {
        super(message, cause);
        this.messageId = messageId;
    }

    public String getMessageId() {
        return messageId;
    }
}
