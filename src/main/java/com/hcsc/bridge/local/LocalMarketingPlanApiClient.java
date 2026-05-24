package com.hcsc.bridge.local;

import com.hcsc.bridge.api.EnrichmentException;
import com.hcsc.bridge.api.MarketingPlanApiClient;
import com.hcsc.bridge.model.ParsedPayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Component
@Profile("local")
public class LocalMarketingPlanApiClient implements MarketingPlanApiClient {

    private static final Logger logger = LoggerFactory.getLogger(LocalMarketingPlanApiClient.class);

    @Override
    public EnrichmentResult enrich(ParsedPayload payload) throws EnrichmentException {
        logger.debug("Local enrichment for entityId: {}", payload.getEntityId());

        Map<String, Object> additionalData = new HashMap<>();
        additionalData.put("enrichedAt", System.currentTimeMillis());
        additionalData.put("source", "local-mock");
        additionalData.put("environment", "local");

        String marketingPlanId = "LOCAL-MP-" + payload.getTransactionId();
        String campaignId = "LOCAL-CAMP-" + payload.getEntityId();

        return new EnrichmentResult(marketingPlanId, campaignId, additionalData);
    }
}
