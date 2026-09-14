package com.hcsc.bridge.local;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public EnrichmentResult enrich(ParsedPayload payload) throws EnrichmentException {
        logger.debug("Local enrichment for entityId: {}", payload.getEntityId());

        JsonNode rawResponse = buildRawResponse(payload.getEntityId());
        String marketingPlanId = rawResponse
                .path("PlanResponse").path("planIdentification").path("marketingPlanIdentifier")
                .asText(null);

        return new EnrichmentResult(marketingPlanId, null, new HashMap<>(), rawResponse);
    }

    private JsonNode buildRawResponse(String entityId) {
        Map<String, Object> planIdentification = new HashMap<>();
        planIdentification.put("marketingPlanIdentifier", "LOCAL-MP-" + entityId);
        planIdentification.put("planName", "Local Mock Plan");

        Map<String, Object> changeEvent = new HashMap<>();
        changeEvent.put("eventName", "ReadyToSell");
        changeEvent.put("typeName", "Update");
        changeEvent.put("timestamp", "20260710T162108.143 CDT");

        Map<String, Object> planResponse = new HashMap<>();
        planResponse.put("planIdentification", planIdentification);
        planResponse.put("changeEvent", changeEvent);

        Map<String, Object> root = new HashMap<>();
        root.put("PlanResponse", planResponse);

        return objectMapper.valueToTree(root);
    }
}
