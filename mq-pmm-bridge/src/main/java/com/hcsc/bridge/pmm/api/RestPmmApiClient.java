package com.hcsc.bridge.pmm.api;

import com.hcsc.bridge.core.SecretMaskingUtil;
import com.hcsc.bridge.security.JwtTokenProvider;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

/**
 * OkHttp client for the PMM web service. Retry, forced token refresh and
 * retryability classification mirror the PMM+ enrichment client so both bridges
 * behave identically under gateway trouble:
 * <ul>
 *   <li>linear backoff between attempts;</li>
 *   <li>exactly one forced token refresh on the first 401/403;</li>
 *   <li>401/403/408/429, 5xx, timeouts and I/O errors are retryable (message stays on
 *       the queue); other 4xx and an empty 2xx body are permanent (quarantine).</li>
 * </ul>
 */
@Component
@Profile("!local")
public class RestPmmApiClient implements PmmApiClient {

    private static final Logger logger = LoggerFactory.getLogger(RestPmmApiClient.class);

    private final JwtTokenProvider jwtTokenProvider;
    private final String url;
    private final String clientId;
    private final String clientSecret;
    private final OkHttpClient httpClient;
    private final int retryAttempts;
    private final long retryDelayMs;
    private final String contentType;
    private final String accept;

    @Autowired
    public RestPmmApiClient(
            JwtTokenProvider jwtTokenProvider,
            @Value("${bridge.pmm.api.url}") String url,
            @Value("${bridge.security.client-id:}") String clientId,
            @Value("${bridge.security.client-secret:}") String clientSecret,
            @Value("${bridge.pmm.api.timeout-seconds:60}") int timeoutSeconds,
            @Value("${bridge.pmm.api.retry-attempts:3}") int retryAttempts,
            @Value("${bridge.pmm.api.retry-delay-ms:1000}") long retryDelayMs,
            @Value("${bridge.pmm.api.content-type:application/xml}") String contentType,
            @Value("${bridge.pmm.api.accept:application/xml}") String accept) {
        this(jwtTokenProvider, url, clientId, clientSecret,
                new OkHttpClient.Builder()
                        .connectTimeout(timeoutSeconds, TimeUnit.SECONDS)
                        .readTimeout(timeoutSeconds, TimeUnit.SECONDS)
                        .writeTimeout(timeoutSeconds, TimeUnit.SECONDS)
                        // A fixed internal endpoint never legitimately redirects. Following one
                        // would replay the PHI-bearing body and the ClientID/ClientSecret headers
                        // (which OkHttp does NOT strip, unlike Authorization) to the Location host.
                        .followRedirects(false)
                        .followSslRedirects(false)
                        .build(),
                retryAttempts, retryDelayMs, contentType, accept);
    }

    public RestPmmApiClient(JwtTokenProvider jwtTokenProvider, String url, String clientId,
                            String clientSecret, OkHttpClient httpClient, int retryAttempts,
                            long retryDelayMs, String contentType, String accept) {
        this.jwtTokenProvider = jwtTokenProvider;
        this.url = url;
        this.clientId = clientId;
        this.clientSecret = clientSecret;
        this.httpClient = httpClient;
        this.retryAttempts = Math.max(1, retryAttempts);
        this.retryDelayMs = retryDelayMs;
        this.contentType = contentType;
        this.accept = accept;
    }

    @Override
    public PmmApiResponse submit(String requestXml, String eventId) throws PmmApiException {
        PmmApiException lastException = null;
        boolean tokenRefreshed = false;

        for (int attempt = 1; attempt <= retryAttempts; attempt++) {
            try {
                return attemptSubmit(requestXml, eventId, attempt);
            } catch (PmmApiException e) {
                lastException = e;
                // A locally-"valid" cached token can still be rejected (revoked, rotated,
                // clock skew). Force exactly one fresh-token attempt before treating the
                // auth failure as environmental. Not gated on remaining attempts: the
                // fresh token is cached for the redelivery that follows.
                boolean authFailure = e.getStatusCode() == 401 || e.getStatusCode() == 403;
                if (authFailure && !tokenRefreshed) {
                    logger.warn("Auth failure {} on attempt {}/{} for eventId {}; refreshing token and retrying once",
                            e.getStatusCode(), attempt, retryAttempts, eventId);
                    try {
                        jwtTokenProvider.refreshToken();
                        tokenRefreshed = true;
                        continue;
                    } catch (RuntimeException refreshFailure) {
                        logger.warn("Token refresh after {} failed for eventId {}: {}",
                                e.getStatusCode(), eventId, refreshFailure.getMessage());
                        throw new PmmApiException("Auth failure and token refresh failed: "
                                + refreshFailure.getMessage(), eventId, refreshFailure, true);
                    }
                }
                if (!e.isRetryable() || attempt >= retryAttempts) {
                    throw e;
                }
                logger.warn("Retryable error on attempt {}/{} for eventId {}: {}",
                        attempt, retryAttempts, eventId, e.getMessage());
                sleep(retryDelayMs * attempt, eventId);
            }
        }
        throw lastException;
    }

    private PmmApiResponse attemptSubmit(String requestXml, String eventId, int attempt) {
        logger.debug("Submitting PMM request for eventId {} (attempt {}, {} chars)",
                eventId, attempt, requestXml.length());

        // Token-provider failures become a retryable PmmApiException so they take the
        // retry loop, the typed handler and the API_CALL_FAILED audit like any other outage.
        String token;
        try {
            token = jwtTokenProvider.getToken();
        } catch (RuntimeException e) {
            logger.error("Token acquisition failed for eventId {}: {}", eventId, e.getMessage());
            throw new PmmApiException("Failed to acquire API token: " + e.getMessage(), eventId, e, true);
        }

        // Body from bytes, not String: OkHttp appends "; charset=utf-8" to a String body's
        // media type, and the gateway is strict about the exact Content-Type value.
        RequestBody body = RequestBody.create(requestXml.getBytes(StandardCharsets.UTF_8),
                MediaType.parse(contentType));
        Request.Builder requestBuilder = new Request.Builder()
                .url(url)
                .post(body)
                .header("Authorization", "Bearer " + token)
                .header("Accept", accept)
                .header("Content-Type", contentType);
        // ClientID / ClientSecret are gateway credentials; never log them
        if (clientId != null && !clientId.isEmpty()) {
            requestBuilder.header("ClientID", clientId);
        }
        if (clientSecret != null && !clientSecret.isEmpty()) {
            requestBuilder.header("ClientSecret", clientSecret);
        }

        long started = System.nanoTime();
        try (Response response = httpClient.newCall(requestBuilder.build()).execute()) {
            int statusCode = response.code();
            long durationMs = (System.nanoTime() - started) / 1_000_000L;

            if (statusCode >= 200 && statusCode < 300) {
                return readSuccessBody(response, eventId, durationMs);
            } else if (statusCode >= 400 && statusCode < 500) {
                logger.warn("Client error {} for eventId {} — response: {}",
                        statusCode, eventId, safeErrorBody(response));
                // 401/403 (auth outage — one refresh is forced by submit()), 408 and 429
                // are environmental: the message must stay on the queue. Only statuses
                // that can never succeed for THIS message may quarantine.
                boolean transientClientError = statusCode == 401 || statusCode == 403
                        || statusCode == 408 || statusCode == 429;
                throw new PmmApiException("API client error: " + statusCode, eventId, statusCode,
                        transientClientError);
            } else if (statusCode >= 500) {
                logger.warn("Server error {} for eventId {} — response: {}",
                        statusCode, eventId, safeErrorBody(response));
                throw new PmmApiException("API server error: " + statusCode, eventId, statusCode, true);
            } else if (statusCode >= 300 && statusCode < 400) {
                // Redirects are refused (see the client builder) and can never succeed for
                // this message: permanent, so the operator sees it in quarantine
                logger.warn("Redirect {} refused for eventId {} — the PMM endpoint must not redirect", statusCode, eventId);
                throw new PmmApiException("API redirect refused: " + statusCode, eventId, statusCode, false);
            } else {
                throw new PmmApiException("Unexpected response status: " + statusCode, eventId, statusCode, false);
            }
        } catch (SocketTimeoutException e) {
            logger.warn("Timeout submitting eventId {} (attempt {})", eventId, attempt);
            throw new PmmApiException("Request timed out", eventId, e, true);
        } catch (IOException e) {
            logger.warn("IO error submitting eventId {} (attempt {}): {}", eventId, attempt, e.toString());
            throw new PmmApiException("Failed to call PMM API", eventId, e, true);
        }
    }

    private PmmApiResponse readSuccessBody(Response response, String eventId, long durationMs) {
        try {
            String body = response.body() != null ? response.body().string() : "";
            if (body.isEmpty()) {
                // A 200 with no body is the gateway's state, not this message's: retry, and
                // if it persists leave the message on the queue rather than quarantine + ack
                throw new PmmApiException("Empty response body", eventId, response.code(), true);
            }
            logger.debug("PMM API responded {} for eventId {} in {} ms ({} chars)",
                    response.code(), eventId, durationMs, body.length());
            return new PmmApiResponse(response.code(), body, durationMs);
        } catch (IOException e) {
            // A stream error mid-body is the same transient fault class as a reset before
            // the headers — retryable
            throw new PmmApiException("Failed to read response body", eventId, e, true);
        }
    }

    /** Error body for diagnostics: truncated and secret-masked (gateways echo request material). */
    private String safeErrorBody(Response response) {
        try {
            String body = response.body() != null ? response.body().string() : "";
            if (body.isEmpty()) {
                return "<empty>";
            }
            String truncated = body.length() > 300 ? body.substring(0, 300) + "...(truncated)" : body;
            return SecretMaskingUtil.maskSecrets(truncated);
        } catch (IOException e) {
            return "<unreadable: " + e.getMessage() + ">";
        }
    }

    private void sleep(long ms, String eventId) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            // MUST be retryable: an interrupt is a property of this JVM's lifecycle, not of
            // the message — a permanent failure would quarantine + ack a healthy message on
            // every deploy that catches a retry mid-backoff.
            Thread.currentThread().interrupt();
            throw new PmmApiException("PMM API retry interrupted", eventId, e, true);
        }
    }
}
