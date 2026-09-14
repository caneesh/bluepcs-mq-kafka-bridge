package com.hcsc.bridge.pmm.api;

/**
 * Web-service call failure. {@code retryable} decides the disposition upstream:
 * retryable → no ack, MQ redelivers; permanent → quarantine-then-ack.
 */
public class PmmApiException extends RuntimeException {

    private final String eventId;
    private final int statusCode;
    private final boolean retryable;

    public PmmApiException(String message, String eventId, int statusCode, boolean retryable) {
        super(message);
        this.eventId = eventId;
        this.statusCode = statusCode;
        this.retryable = retryable;
    }

    public PmmApiException(String message, String eventId, Throwable cause, boolean retryable) {
        super(message, cause);
        this.eventId = eventId;
        this.statusCode = 0;
        this.retryable = retryable;
    }

    public String getEventId() {
        return eventId;
    }

    public int getStatusCode() {
        return statusCode;
    }

    public boolean isRetryable() {
        return retryable;
    }
}
