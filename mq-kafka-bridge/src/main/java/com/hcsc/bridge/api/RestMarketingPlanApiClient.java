package com.hcsc.bridge.api;

import com.hcsc.bridge.model.ParsedPayload;
import com.hcsc.bridge.security.JwtTokenProvider;
import com.fasterxml.jackson.core.JsonProcessingException;
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
import java.util.concurrent.TimeUnit;

@Component
@Profile("!local")
public class RestMarketingPlanApiClient implements MarketingPlanApiClient {

    private static final Logger logger = LoggerFactory.getLogger(RestMarketingPlanApiClient.class);

    private final JwtTokenProvider jwtTokenProvider;
    private final String baseUrl;
    private final String clientId;
    private final String clientSecret;
    private final OkHttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final int retryAttempts;
    private final long retryDelayMs;

    @Autowired
    public RestMarketingPlanApiClient(
            JwtTokenProvider jwtTokenProvider,
            @Value("${bridge.api.base-url:http://localhost:8080}") String baseUrl,
            @Value("${bridge.security.client-id:}") String clientId,
            @Value("${bridge.security.client-secret:}") String clientSecret,
            @Value("${bridge.api.timeout-seconds:30}") int timeoutSeconds,
            @Value("${bridge.api.retry-attempts:3}") int retryAttempts,
            @Value("${bridge.api.retry-delay-ms:1000}") long retryDelayMs) {
        this.jwtTokenProvider = jwtTokenProvider;
        this.baseUrl = baseUrl;
        this.clientId = clientId;
        this.clientSecret = clientSecret;
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
            String clientId,
            String clientSecret,
            OkHttpClient httpClient) {
        this.jwtTokenProvider = jwtTokenProvider;
        this.baseUrl = baseUrl;
        this.clientId = clientId;
        this.clientSecret = clientSecret;
        this.httpClient = httpClient;
        this.objectMapper = new ObjectMapper();
        this.retryAttempts = 3;
        this.retryDelayMs = 1000;
    }

    @Override
    public EnrichmentResult enrich(ParsedPayload payload) throws EnrichmentException {
        String entityId = payload.getEntityId();
        EnrichmentException lastException = null;
        boolean tokenRefreshed = false;

        for (int attempt = 1; attempt <= retryAttempts; attempt++) {
            try {
                return attemptEnrich(payload, entityId, attempt);
            } catch (EnrichmentException e) {
                lastException = e;
                // A locally-"valid" cached token can still be rejected (revoked, rotated,
                // clock skew, overstated expires_in). Without invalidation every message
                // 401s against the same cached token until natural expiry — force exactly
                // one fresh-token attempt before treating the auth failure as permanent.
                // Not gated on remaining attempts: even on the final attempt (or with
                // retry-attempts=1) the refresh is worth doing — the fresh token is cached
                // for the redelivery that follows a retryable failure.
                boolean authFailure = e.getStatusCode() == 401 || e.getStatusCode() == 403;
                if (authFailure && !tokenRefreshed) {
                    logger.warn("Auth failure {} on attempt {}/{} for entityId {}; refreshing "
                                    + "token and retrying once",
                            e.getStatusCode(), attempt, retryAttempts, entityId);
                    try {
                        jwtTokenProvider.refreshToken();
                        tokenRefreshed = true;
                        continue;
                    } catch (RuntimeException refreshFailure) {
                        logger.warn("Token refresh after {} failed for entityId {}: {}",
                                e.getStatusCode(), entityId, refreshFailure.getMessage());
                        throw new EnrichmentException("Auth failure and token refresh failed: "
                                + refreshFailure.getMessage(), entityId, refreshFailure, true);
                    }
                }
                if (!e.isRetryable() || attempt >= retryAttempts) {
                    throw e;
                }
                logger.warn("Retryable error on attempt {}/{} for entityId {}: {}",
                        attempt, retryAttempts, entityId, e.getMessage());
                sleep(retryDelayMs * attempt, entityId);
            }
        }
        throw lastException;
    }

    private EnrichmentResult attemptEnrich(ParsedPayload payload, String entityId, int attempt)
            throws EnrichmentException {
        HttpUrl base = HttpUrl.parse(baseUrl);
        if (base == null) {
            throw new EnrichmentException("Invalid URL for enrichment", entityId);
        }
        // The base URL points at the plans resource (.../plans). addPathSegment percent-encodes
        // entityId and effectiveDate, which come from the MQ payload and must not be able to
        // alter the request path (e.g. via '/', '?' or '../')
        HttpUrl url = base.newBuilder()
                .addPathSegment(payload.getEntityId())
                .addPathSegment(payload.getEffectiveDate())
                .build();

        logger.debug("Enriching payload for entityId: {} (attempt {})", entityId, attempt);

        // Translate token-provider failures (e.g. TokenRefreshException) into a retryable
        // EnrichmentException: a bare RuntimeException would bypass the retry loop, the
        // orchestrator's typed handlers and the ENRICHMENT_FAILED audit entirely.
        String token;
        try {
            token = jwtTokenProvider.getToken();
        } catch (RuntimeException e) {
            logger.error("Token acquisition failed for entityId {}: {}", entityId, e.getMessage());
            throw new EnrichmentException("Failed to acquire API token: " + e.getMessage(), entityId, e, true);
        }

        Request.Builder requestBuilder = new Request.Builder()
                .url(url)
                .get()
                .header("Authorization", "Bearer " + token)
                .header("Accept", "application/json")
                // Sent despite the empty GET body: the gateway is strict about content
                // types and the verified working request includes it
                .header("Content-Type", "application/json");
        // ClientID / ClientSecret are credentials required by the API gateway; never log them
        if (clientId != null && !clientId.isEmpty()) {
            requestBuilder.header("ClientID", clientId);
        }
        if (clientSecret != null && !clientSecret.isEmpty()) {
            requestBuilder.header("ClientSecret", clientSecret);
        }
        Request request = requestBuilder.build();

        try (Response response = httpClient.newCall(request).execute()) {
            int statusCode = response.code();

            if (statusCode >= 200 && statusCode < 300) {
                return parseSuccessResponse(response, entityId);
            } else if (statusCode >= 400 && statusCode < 500) {
                logger.warn("Client error {} for entityId: {} — response: {}",
                        statusCode, entityId, safeErrorBody(response));
                // Retryability decides quarantine-vs-redeliver in the orchestrator. 401/403
                // (auth outage — refresh is forced once by enrich(), a persisting failure is
                // the gateway's problem, not this message's) and 408/429 (timeout/throttle)
                // are environmental: the message must stay on the queue and redeliver. Only
                // statuses that can never succeed for THIS message (400/404/422/...) may
                // quarantine, because quarantine is followed by an MQ ack.
                boolean transientClientError = statusCode == 401 || statusCode == 403
                        || statusCode == 408 || statusCode == 429;
                throw new EnrichmentException(
                        "API client error: " + statusCode,
                        entityId,
                        statusCode,
                        transientClientError
                );
            } else if (statusCode >= 500) {
                // WARN per attempt — the orchestrator emits the single terminal ERROR
                logger.warn("Server error {} for entityId: {} — response: {}",
                        statusCode, entityId, safeErrorBody(response));
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
            logger.warn("Timeout enriching entityId: {} (attempt {})", entityId, attempt);
            throw new EnrichmentException("Request timed out", entityId, e, true);
        } catch (IOException e) {
            logger.warn("IO error enriching entityId: {} (attempt {}): {}", entityId, attempt, e.toString());
            throw new EnrichmentException("Failed to call enrichment API", entityId, e, true);
        }
    }

    /**
     * Reads the error response body for diagnostics, truncated to a safe length.
     * Gateway error bodies explain WHY a request was rejected (invalid token,
     * missing client credentials, unknown plan, ...), which the status alone hides.
     */
    private String safeErrorBody(Response response) {
        try {
            String body = response.body() != null ? response.body().string() : "";
            if (body.isEmpty()) {
                return "<empty>";
            }
            String truncated = body.length() > 300 ? body.substring(0, 300) + "...(truncated)" : body;
            // Same reasoning as the STS client: a misbehaving gateway can echo request
            // material (ClientID/ClientSecret headers, bearer token) back in error pages
            return com.hcsc.bridge.core.SecretMaskingUtil.maskSecrets(truncated);
        } catch (IOException e) {
            return "<unreadable: " + e.getMessage() + ">";
        }
    }

    private void sleep(long ms, String entityId) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            // Abort the retry loop immediately (e.g. on shutdown). MUST be retryable: an
            // interrupt is a property of this JVM's lifecycle, not of the message — marking
            // it permanent would quarantine + ack a healthy message on every deploy that
            // catches a retry mid-backoff. Retryable → no ack → the broker redelivers
            // after restart.
            Thread.currentThread().interrupt();
            throw new EnrichmentException("Enrichment retry interrupted", entityId, e, true);
        }
    }

    private EnrichmentResult parseSuccessResponse(Response response, String entityId) throws EnrichmentException {
        try {
            String body = response.body() != null ? response.body().string() : "";
            if (body.isEmpty()) {
                throw new EnrichmentException("Empty response body", entityId, 200, false);
            }

            // Parse the response body once into its unmodified root. This node (whose single
            // child is PlanResponse) is carried through as rawResponse and later attached
            // verbatim to the published wrapper.
            JsonNode root = objectMapper.readTree(body);

            // marketingPlanId is extracted null-safely from the real response structure.
            // campaignId has no source in the real response; additionalData's only consumer
            // (the previous HDFS serialization) is being replaced, so it is now empty.
            JsonNode marketingPlanIdentifier = root
                    .path("PlanResponse")
                    .path("planIdentification")
                    .path("marketingPlanIdentifier");
            String marketingPlanId = marketingPlanIdentifier.isTextual()
                    ? marketingPlanIdentifier.asText()
                    : null;

            logger.debug("Enrichment successful for entityId: {}", entityId);
            return new EnrichmentResult(marketingPlanId, null, new HashMap<>(), root);

        } catch (JsonProcessingException e) {
            // Malformed JSON in a 2xx body is permanent — retrying gets the same bytes
            throw new EnrichmentException("Failed to parse response", entityId, e, false);
        } catch (IOException e) {
            // A stream error reading a 2xx body (connection reset mid-body) is the same
            // transient network fault class as a reset before the headers — retryable
            throw new EnrichmentException("Failed to read response body", entityId, e, true);
        }
    }
}
