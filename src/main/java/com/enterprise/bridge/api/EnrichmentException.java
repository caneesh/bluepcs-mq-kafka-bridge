package com.enterprise.bridge.api;

public class EnrichmentException extends RuntimeException {

    private final String entityId;
    private final int statusCode;
    private final boolean retryable;

    public EnrichmentException(String message, String entityId) {
        super(message);
        this.entityId = entityId;
        this.statusCode = 0;
        this.retryable = false;
    }

    public EnrichmentException(String message, String entityId, int statusCode) {
        super(message);
        this.entityId = entityId;
        this.statusCode = statusCode;
        this.retryable = statusCode >= 500;
    }

    public EnrichmentException(String message, String entityId, int statusCode, boolean retryable) {
        super(message);
        this.entityId = entityId;
        this.statusCode = statusCode;
        this.retryable = retryable;
    }

    public EnrichmentException(String message, String entityId, Throwable cause) {
        super(message, cause);
        this.entityId = entityId;
        this.statusCode = 0;
        this.retryable = true;
    }

    public EnrichmentException(String message, String entityId, Throwable cause, boolean retryable) {
        super(message, cause);
        this.entityId = entityId;
        this.statusCode = 0;
        this.retryable = retryable;
    }

    public String getEntityId() {
        return entityId;
    }

    public int getStatusCode() {
        return statusCode;
    }

    public boolean isRetryable() {
        return retryable;
    }
}
