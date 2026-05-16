package com.enterprise.bridge.api;

import com.enterprise.bridge.model.ParsedPayload;
import com.enterprise.bridge.security.JwtTokenProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import java.util.HashMap;
import java.util.Map;

@Component
public class RestMarketingPlanApiClient implements MarketingPlanApiClient {

    private static final Logger logger = LoggerFactory.getLogger(RestMarketingPlanApiClient.class);

    private final RestTemplate restTemplate;
    private final JwtTokenProvider jwtTokenProvider;
    private final String baseUrl;

    public RestMarketingPlanApiClient(
            RestTemplate restTemplate,
            JwtTokenProvider jwtTokenProvider,
            @Value("${bridge.api.base-url:http://localhost:8080}") String baseUrl) {
        this.restTemplate = restTemplate;
        this.jwtTokenProvider = jwtTokenProvider;
        this.baseUrl = baseUrl;
    }

    @Override
    @SuppressWarnings("unchecked")
    public EnrichmentResult enrich(ParsedPayload payload) throws EnrichmentException {
        String entityId = payload.getEntityId();
        String url = baseUrl + "/api/v1/marketing-plans/" + entityId;

        logger.debug("Enriching payload for entityId: {}", entityId);

        try {
            HttpHeaders headers = new HttpHeaders();
            headers.setBearerAuth(jwtTokenProvider.getToken());
            HttpEntity<Void> request = new HttpEntity<>(headers);

            ResponseEntity<Map> response = restTemplate.exchange(
                    url, HttpMethod.GET, request, Map.class);

            if (response.getStatusCode() == HttpStatus.OK && response.getBody() != null) {
                Map<String, Object> body = response.getBody();
                String marketingPlanId = (String) body.get("marketingPlanId");
                String campaignId = (String) body.get("campaignId");

                Map<String, Object> additionalData = new HashMap<>(body);
                additionalData.remove("marketingPlanId");
                additionalData.remove("campaignId");

                logger.debug("Enrichment successful for entityId: {}", entityId);
                return new EnrichmentResult(marketingPlanId, campaignId, additionalData);
            }

            throw new EnrichmentException("Unexpected response from API", entityId);

        } catch (HttpClientErrorException e) {
            logger.error("API client error for entityId {}: {}", entityId, e.getStatusCode());
            throw new EnrichmentException("API returned error: " + e.getStatusCode(),
                    entityId, e.getRawStatusCode());
        } catch (RestClientException e) {
            logger.error("Failed to enrich payload for entityId: {}", entityId, e);
            throw new EnrichmentException("Failed to call enrichment API", entityId, e);
        }
    }
}
