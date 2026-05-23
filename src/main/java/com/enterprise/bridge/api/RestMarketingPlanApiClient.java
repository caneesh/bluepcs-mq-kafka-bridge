package com.enterprise.bridge.api;

import com.enterprise.bridge.model.ParsedPayload;
import com.enterprise.bridge.security.JwtTokenProvider;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Component
@Profile("!local")
public class RestMarketingPlanApiClient implements MarketingPlanApiClient {

    private static final Logger logger = LoggerFactory.getLogger(RestMarketingPlanApiClient.class);

    private final JwtTokenProvider jwtTokenProvider;
    private final String baseUrl;
    private final OkHttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final int retryAttempts;
    private final long retryDelayMs;

    @Autowired
    public RestMarketingPlanApiClient(
            JwtTokenProvider jwtTokenProvider,
            @Value("${bridge.api.base-url:http://localhost:8080}") String baseUrl,
            @Value("${bridge.api.timeout-seconds:30}") int timeoutSeconds,
            @Value("${bridge.api.retry-attempts:3}") int retryAttempts,
            @Value("${bridge.api.retry-delay-ms:1000}") long retryDelayMs) {
        this.jwtTokenProvider = jwtTokenProvider;
        this.baseUrl = baseUrl;
        this.httpClient = new OkHttpClient.Builder()
                .connectTimeout(timeoutSeconds, TimeUnit.SECONDS)
                .readTimeout(timeoutSeconds, TimeUnit.SECONDS)
                .writeTimeout(timeoutSeconds, TimeUnit.SECONDS)
                .build();
        this.objectMapper = new ObjectMapper();
        this.retryAttempts = retryAttempts;
        this.retryDelayMs = retryDelayMs;
    }

    public RestMarketingPlanApiClient(
            JwtTokenProvider jwtTokenProvider,
            String baseUrl,
            OkHttpClient httpClient) {
        this.jwtTokenProvider = jwtTokenProvider;
        this.baseUrl = baseUrl;
        this.httpClient = httpClient;
        this.objectMapper = new ObjectMapper();
        this.retryAttempts = 3;
        this.retryDelayMs = 1000;
    }

    @Override
    public EnrichmentResult enrich(ParsedPayload payload) throws EnrichmentException {
        String entityId = payload.getEntityId();
        EnrichmentException lastException = null;

        for (int attempt = 1; attempt <= retryAttempts; attempt++) {
            try {
                return attemptEnrich(payload, entityId, attempt);
            } catch (EnrichmentException e) {
                lastException = e;
                if (!e.isRetryable() || attempt >= retryAttempts) {
                    throw e;
                }
                logger.warn("Retryable error on attempt {}/{} for entityId {}: {}",
                        attempt, retryAttempts, entityId, e.getMessage());
                sleep(retryDelayMs * attempt);
            }
        }
        throw lastException;
    }

    private EnrichmentResult attemptEnrich(ParsedPayload payload, String entityId, int attempt)
            throws EnrichmentException {
        HttpUrl url = HttpUrl.parse(baseUrl + "/api/v1/marketing-plans/" + entityId);
        if (url == null) {
            throw new EnrichmentException("Invalid URL for enrichment", entityId);
        }

        logger.debug("Enriching payload for entityId: {} (attempt {})", entityId, attempt);

        Request request = new Request.Builder()
                .url(url)
                .get()
                .header("Authorization", "Bearer " + jwtTokenProvider.getToken())
                .header("Accept", "application/json")
                .build();

        try (Response response = httpClient.newCall(request).execute()) {
            int statusCode = response.code();

            if (statusCode >= 200 && statusCode < 300) {
                return parseSuccessResponse(response, entityId);
            } else if (statusCode >= 400 && statusCode < 500) {
                logger.warn("Client error {} for entityId: {}", statusCode, entityId);
                throw new EnrichmentException(
                        "API client error: " + statusCode,
                        entityId,
                        statusCode,
                        false
                );
            } else if (statusCode >= 500) {
                logger.error("Server error {} for entityId: {}", statusCode, entityId);
                throw new EnrichmentException(
                        "API server error: " + statusCode,
                        entityId,
                        statusCode,
                        true
                );
            } else {
                throw new EnrichmentException(
                        "Unexpected response status: " + statusCode,
                        entityId,
                        statusCode
                );
            }
        } catch (SocketTimeoutException e) {
            logger.error("Timeout enriching entityId: {}", entityId);
            throw new EnrichmentException("Request timed out", entityId, e, true);
        } catch (IOException e) {
            logger.error("IO error enriching entityId: {}", entityId, e);
            throw new EnrichmentException("Failed to call enrichment API", entityId, e, true);
        }
    }

    private void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private EnrichmentResult parseSuccessResponse(Response response, String entityId) throws EnrichmentException {
        try {
            String body = response.body() != null ? response.body().string() : "";
            if (body.isEmpty()) {
                throw new EnrichmentException("Empty response body", entityId, 200, false);
            }

            JsonNode json = objectMapper.readTree(body);

            String marketingPlanId = json.has("marketingPlanId") ?
                    json.get("marketingPlanId").asText() : null;
            String campaignId = json.has("campaignId") ?
                    json.get("campaignId").asText() : null;

            Map<String, Object> additionalData = new HashMap<>();
            Iterator<String> fieldNames = json.fieldNames();
            while (fieldNames.hasNext()) {
                String fieldName = fieldNames.next();
                if (!"marketingPlanId".equals(fieldName) && !"campaignId".equals(fieldName)) {
                    JsonNode value = json.get(fieldName);
                    if (value.isTextual()) {
                        additionalData.put(fieldName, value.asText());
                    } else if (value.isNumber()) {
                        additionalData.put(fieldName, value.numberValue());
                    } else if (value.isBoolean()) {
                        additionalData.put(fieldName, value.asBoolean());
                    } else {
                        additionalData.put(fieldName, value.toString());
                    }
                }
            }

            logger.debug("Enrichment successful for entityId: {}", entityId);
            return new EnrichmentResult(marketingPlanId, campaignId, additionalData);

        } catch (IOException e) {
            throw new EnrichmentException("Failed to parse response", entityId, e, false);
        }
    }
}
