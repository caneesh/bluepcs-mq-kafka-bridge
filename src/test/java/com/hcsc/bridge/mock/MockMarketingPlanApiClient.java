package com.hcsc.bridge.mock;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hcsc.bridge.api.EnrichmentException;
import com.hcsc.bridge.api.MarketingPlanApiClient;
import com.hcsc.bridge.model.ParsedPayload;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class MockMarketingPlanApiClient implements MarketingPlanApiClient {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

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

        JsonNode rawResponse = buildRawResponse(payload);
        String marketingPlanId = rawResponse
                .path("PlanResponse").path("planIdentification").path("marketingPlanIdentifier")
                .asText(null);

        return new EnrichmentResult(marketingPlanId, null, new HashMap<>(), rawResponse);
    }

    private JsonNode buildRawResponse(ParsedPayload payload) {
        Map<String, Object> planIdentification = new HashMap<>();
        planIdentification.put("marketingPlanIdentifier", "MP-" + payload.getTransactionId());
        planIdentification.put("planName", "Mock Plan for " + payload.getEntityId());

        Map<String, Object> changeEvent = new HashMap<>();
        changeEvent.put("eventName", "ReadyToSell");
        changeEvent.put("typeName", "Update");
        changeEvent.put("timestamp", "20260710T162108.143 CDT");

        Map<String, Object> planResponse = new HashMap<>();
        planResponse.put("planIdentification", planIdentification);
        planResponse.put("changeEvent", changeEvent);

        Map<String, Object> root = new HashMap<>();
        root.put("PlanResponse", planResponse);

        return OBJECT_MAPPER.valueToTree(root);
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
