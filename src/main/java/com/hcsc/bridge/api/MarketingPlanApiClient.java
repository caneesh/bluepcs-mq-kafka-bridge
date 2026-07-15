package com.hcsc.bridge.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.hcsc.bridge.model.ParsedPayload;

import java.util.Map;

public interface MarketingPlanApiClient {

    EnrichmentResult enrich(ParsedPayload payload) throws EnrichmentException;

    class EnrichmentResult {
        private final String marketingPlanId;
        private final String campaignId;
        private final Map<String, Object> additionalData;
        private final JsonNode rawResponse;

        public EnrichmentResult(String marketingPlanId, String campaignId,
                                Map<String, Object> additionalData, JsonNode rawResponse) {
            this.marketingPlanId = marketingPlanId;
            this.campaignId = campaignId;
            this.additionalData = additionalData;
            this.rawResponse = rawResponse;
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

        /**
         * The parsed, unmodified API response root (the node whose single child is
         * {@code PlanResponse}). Used as the source for the published wrapper.
         */
        public JsonNode getRawResponse() {
            return rawResponse;
        }
    }
}
