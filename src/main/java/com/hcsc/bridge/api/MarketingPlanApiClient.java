package com.hcsc.bridge.api;

import com.hcsc.bridge.model.ParsedPayload;

import java.util.Map;

public interface MarketingPlanApiClient {

    EnrichmentResult enrich(ParsedPayload payload) throws EnrichmentException;

    class EnrichmentResult {
        private final String marketingPlanId;
        private final String campaignId;
        private final Map<String, Object> additionalData;

        public EnrichmentResult(String marketingPlanId, String campaignId, Map<String, Object> additionalData) {
            this.marketingPlanId = marketingPlanId;
            this.campaignId = campaignId;
            this.additionalData = additionalData;
        }

        public String getMarketingPlanId() {
            return marketingPlanId;
        }

        public String getCampaignId() {
            return campaignId;
        }

        public Map<String, Object> getAdditionalData() {
            return additionalData;
        }
    }
}
