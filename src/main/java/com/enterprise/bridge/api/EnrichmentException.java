package com.enterprise.bridge.api;

public class EnrichmentException extends RuntimeException {

    private final String entityId;
    private final int statusCode;

    public EnrichmentException(String message, String entityId) {
        super(message);
        this.entityId = entityId;
        this.statusCode = 0;
    }

    public EnrichmentException(String message, String entityId, int statusCode) {
        super(message);
        this.entityId = entityId;
        this.statusCode = statusCode;
    }

    public EnrichmentException(String message, String entityId, Throwable cause) {
        super(message, cause);
        this.entityId = entityId;
        this.statusCode = 0;
    }

    public String getEntityId() {
        return entityId;
    }

    public int getStatusCode() {
        return statusCode;
    }
}
