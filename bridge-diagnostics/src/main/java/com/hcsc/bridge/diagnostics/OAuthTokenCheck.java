package com.hcsc.bridge.diagnostics;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.hcsc.bridge.core.SecretMaskingUtil;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

/**
 * Proves the STS issues a token the bridge can actually use. A 2xx alone is not enough:
 * the body must carry a token field the runtime provider accepts, or validate-only would
 * approve a deployment whose first message fails.
 */
// Ordered so validate-only output reads in the sequence operators are used to:
// the message source first, then the destinations, then the credentials.
@Component
@Order(40)
public class OAuthTokenCheck implements ReadinessCheck {

    private static final Logger logger = LoggerFactory.getLogger(OAuthTokenCheck.class);

    @Value("${bridge.security.token-url:}")
    private String oauthTokenUrl;

    @Value("${bridge.security.client-id:}")
    private String oauthClientId;

    @Value("${bridge.security.client-secret:}")
    private String oauthClientSecret;

    @Value("${bridge.security.scope:}")
    private String oauthScope;

    @Value("${bridge.security.username:}")
    private String oauthUsername;

    @Value("${bridge.security.password:}")
    private String oauthPassword;

    private final RestTemplate restTemplate;

    public OAuthTokenCheck() {
        // Explicit timeouts: a default RestTemplate has NONE, and an STS that accepts the
        // TCP connection but never responds would hang validate-only mode forever - the one
        // check here that can block, while every socket probe uses 5s.
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(10_000);
        factory.setReadTimeout(10_000);
        this.restTemplate = new RestTemplate(factory);
    }


    @Override
    public String name() {
        return "OAUTH_TOKEN";
    }

    @Override
    public CheckResult run() {
        String name = name();

        if (isBlank(oauthTokenUrl) || isBlank(oauthClientId) || isBlank(oauthClientSecret)) {
            return CheckResult.skip(name, "OAuth not fully configured");
        }

        try {
            // The STS expects client credentials + scope as headers and a JSON body
            // with username/password (form-encoding is rejected with 415)
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            headers.set("ClientID", oauthClientId);
            headers.set("ClientSecret", oauthClientSecret);
            if (!isBlank(oauthScope)) {
                headers.set("scope", oauthScope);
            }

            // Build with Jackson so quotes/backslashes in credentials stay valid JSON
            ObjectNode bodyNode = new ObjectMapper().createObjectNode();
            bodyNode.put("username", oauthUsername);
            bodyNode.put("password", oauthPassword);
            String body = bodyNode.toString();

            HttpEntity<String> request = new HttpEntity<>(body, headers);
            ResponseEntity<String> response = restTemplate.postForEntity(oauthTokenUrl, request, String.class);

            if (response.getStatusCode().is2xxSuccessful()) {
                // A 2xx alone proves nothing: an STS behind a gateway can answer 200 with
                // {} or an HTML page. Apply the runtime provider's own acceptance rule so
                // validate-only cannot approve a deployment whose first message would fail.
                String token = com.hcsc.bridge.security.OAuth2JwtTokenProvider.tokenFrom(response.getBody());
                if (token == null) {
                    String message = "OAuth token endpoint answered " + response.getStatusCode()
                            + " but the body carries no recognized token field (fields: "
                            + responseFieldNames(response.getBody()) + ")";
                    logger.error("[FAIL] {}: {}", name, message);
                    return CheckResult.fail(name, message);
                }
                String message = "OAuth token acquired successfully (" + token.length() + " chars)";
                logger.info("[PASS] {}: {}", name, message);
                return CheckResult.pass(name, message);
            } else {
                String message = String.format("OAuth token request failed with status %s", response.getStatusCode());
                logger.error("[FAIL] {}: {}", name, message);
                return CheckResult.fail(name, message);
            }
        } catch (Exception e) {
            // Spring's HttpStatusCodeException.getMessage() embeds the raw response body.
            // This is the same STS whose error bodies can echo the credential headers it
            // rejected — the reason the token provider and API client truncate+mask theirs.
            String detail = e.getMessage();
            if (detail != null && detail.length() > 300) {
                detail = detail.substring(0, 300) + "...(truncated)";
            }
            String message = "OAuth token acquisition failed - " + SecretMaskingUtil.maskSecrets(detail);
            logger.error("[FAIL] {}: {}", name, message);
            return CheckResult.fail(name, message);
        }
    }
    /** Field NAMES of a JSON body for diagnostics (never values: one could be the token). */
    private static String responseFieldNames(String body) {
        try {
            com.fasterxml.jackson.databind.JsonNode json = new ObjectMapper().readTree(body == null ? "" : body);
            if (json == null || !json.isObject()) {
                return "<not a JSON object>";
            }
            List<String> names = new ArrayList<>();
            json.fieldNames().forEachRemaining(names::add);
            return names.isEmpty() ? "<none>" : String.join(",", names);
        } catch (Exception e) {
            return "<unparseable>";
        }
    }
    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }
}
