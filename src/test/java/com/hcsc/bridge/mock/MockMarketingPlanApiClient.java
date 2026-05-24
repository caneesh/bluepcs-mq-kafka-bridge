package com.hcsc.bridge.mock;

import com.hcsc.bridge.api.EnrichmentException;
import com.hcsc.bridge.api.MarketingPlanApiClient;
import com.hcsc.bridge.model.ParsedPayload;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class MockMarketingPlanApiClient implements MarketingPlanApiClient {

    private boolean shouldFail = false;
    private boolean shouldTimeout = false;
    private String failureMessage = "Mock API failure";
    private int timeoutDelayMs = 5000;
    private final AtomicInteger callCount = new AtomicInteger(0);

    @Override
    public EnrichmentResult enrich(ParsedPayload payload) throws EnrichmentException {
        callCount.incrementAndGet();

        if (shouldTimeout) {
            try {
                Thread.sleep(timeoutDelayMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new EnrichmentException("Timeout during enrichment", payload.getEntityId());
            }
            throw new EnrichmentException("Request timed out", payload.getEntityId());
        }

        if (shouldFail) {
            throw new EnrichmentException(failureMessage, payload.getEntityId());
        }

        Map<String, Object> additionalData = new HashMap<>();
        additionalData.put("enrichedAt", System.currentTimeMillis());
        additionalData.put("source", "mock-api");
        additionalData.put("entityId", payload.getEntityId());
        additionalData.put("version", "1.0");

        String marketingPlanId = "MP-" + payload.getTransactionId();
        String campaignId = "CAMP-" + payload.getEntityId();

        return new EnrichmentResult(marketingPlanId, campaignId, additionalData);
    }

    public void setShouldFail(boolean shouldFail) {
        this.shouldFail = shouldFail;
    }

    public void setShouldTimeout(boolean shouldTimeout) {
        this.shouldTimeout = shouldTimeout;
    }

    public void setFailureMessage(String failureMessage) {
        this.failureMessage = failureMessage;
    }

    public void setTimeoutDelayMs(int timeoutDelayMs) {
        this.timeoutDelayMs = timeoutDelayMs;
    }

    public int getCallCount() {
        return callCount.get();
    }

    public void reset() {
        shouldFail = false;
        shouldTimeout = false;
        failureMessage = "Mock API failure";
        timeoutDelayMs = 5000;
        callCount.set(0);
    }
}
