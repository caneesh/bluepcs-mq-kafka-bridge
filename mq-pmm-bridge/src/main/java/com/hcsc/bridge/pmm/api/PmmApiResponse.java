package com.hcsc.bridge.pmm.api;

import java.util.Objects;

/** A successful (2xx, non-empty) web-service response, body carried verbatim. */
public final class PmmApiResponse {

    private final int statusCode;
    private final String body;
    private final long durationMs;

    public PmmApiResponse(int statusCode, String body, long durationMs) {
        this.statusCode = statusCode;
        this.body = Objects.requireNonNull(body, "body must not be null");
        this.durationMs = durationMs;
    }

    public int getStatusCode() {
        return statusCode;
    }

    public String getBody() {
        return body;
    }

    public long getDurationMs() {
        return durationMs;
    }
}
