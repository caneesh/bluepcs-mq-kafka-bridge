package com.enterprise.bridge.model;

import com.enterprise.bridge.core.ProcessingContext;

import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

public final class EnrichedPayload {

    private final ParsedPayload parsedPayload;
    private final ProcessingContext processingContext;
    private final Map<String, Object> enrichmentData;
    private final String marketingPlanId;
    private final String campaignId;
    private final Instant enrichedAt;

    public EnrichedPayload(ParsedPayload parsedPayload, ProcessingContext processingContext,
                           Map<String, Object> enrichmentData, String marketingPlanId,
                           String campaignId, Instant enrichedAt) {
        this.parsedPayload = Objects.requireNonNull(parsedPayload, "parsedPayload must not be null");
        this.processingContext = Objects.requireNonNull(processingContext, "processingContext must not be null");
        this.enrichmentData = enrichmentData != null ? Collections.unmodifiableMap(enrichmentData) : Collections.emptyMap();
        this.marketingPlanId = marketingPlanId;
        this.campaignId = campaignId;
        this.enrichedAt = Objects.requireNonNull(enrichedAt, "enrichedAt must not be null");
    }

    public ParsedPayload getParsedPayload() {
        return parsedPayload;
    }

    public ProcessingContext getProcessingContext() {
        return processingContext;
    }

    public Map<String, Object> getEnrichmentData() {
        return enrichmentData;
    }

    public String getMarketingPlanId() {
        return marketingPlanId;
    }

    public String getCampaignId() {
        return campaignId;
    }

    public Instant getEnrichedAt() {
        return enrichedAt;
    }

    public String getEventId() {
        return processingContext.getEventId();
    }

    public String getBridgeMessageId() {
        return processingContext.getBridgeMessageId();
    }

    public String getOriginalMqMessageId() {
        return processingContext.getOriginalMqMessageId();
    }

    public String getMessageId() {
        return parsedPayload.getMessageId();
    }

    public String getTransactionId() {
        return parsedPayload.getTransactionId();
    }

    public String getEventType() {
        return parsedPayload.getEventType();
    }

    public String getEntityId() {
        return parsedPayload.getEntityId();
    }

    public String getRawPayload() {
        return parsedPayload.getRawPayload();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EnrichedPayload that = (EnrichedPayload) o;
        return Objects.equals(processingContext.getEventId(), that.processingContext.getEventId());
    }

    @Override
    public int hashCode() {
        return Objects.hash(processingContext.getEventId());
    }

    @Override
    public String toString() {
        return "EnrichedPayload{" +
                "eventId='" + processingContext.getEventId() + '\'' +
                ", transactionId='" + parsedPayload.getTransactionId() + '\'' +
                ", marketingPlanId='" + marketingPlanId + '\'' +
                ", campaignId='" + campaignId + '\'' +
                ", enrichedAt=" + enrichedAt +
                '}';
    }
}
