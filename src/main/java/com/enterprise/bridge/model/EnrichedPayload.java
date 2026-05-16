package com.enterprise.bridge.model;

import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

public final class EnrichedPayload {

    private final ParsedPayload parsedPayload;
    private final Map<String, Object> enrichmentData;
    private final String marketingPlanId;
    private final String campaignId;
    private final Instant enrichedAt;

    public EnrichedPayload(ParsedPayload parsedPayload, Map<String, Object> enrichmentData,
                           String marketingPlanId, String campaignId, Instant enrichedAt) {
        this.parsedPayload = Objects.requireNonNull(parsedPayload, "parsedPayload must not be null");
        this.enrichmentData = enrichmentData != null ? Collections.unmodifiableMap(enrichmentData) : Collections.emptyMap();
        this.marketingPlanId = marketingPlanId;
        this.campaignId = campaignId;
        this.enrichedAt = Objects.requireNonNull(enrichedAt, "enrichedAt must not be null");
    }

    public ParsedPayload getParsedPayload() {
        return parsedPayload;
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
        return Objects.equals(parsedPayload, that.parsedPayload);
    }

    @Override
    public int hashCode() {
        return Objects.hash(parsedPayload);
    }

    @Override
    public String toString() {
        return "EnrichedPayload{" +
                "parsedPayload=" + parsedPayload +
                ", marketingPlanId='" + marketingPlanId + '\'' +
                ", campaignId='" + campaignId + '\'' +
                ", enrichedAt=" + enrichedAt +
                '}';
    }
}
