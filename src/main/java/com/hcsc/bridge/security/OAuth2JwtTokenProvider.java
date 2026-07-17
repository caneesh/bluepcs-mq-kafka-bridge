package com.hcsc.bridge.security;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
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

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.time.Instant;
import java.util.Base64;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Component
@Profile("!local")
public class OAuth2JwtTokenProvider implements JwtTokenProvider {

    private static final Logger logger = LoggerFactory.getLogger(OAuth2JwtTokenProvider.class);
    private static final long EXPIRY_BUFFER_SECONDS = 60;

    private final String tokenUrl;
    private final String clientId;
    private final String clientSecret;
    private final String scope;
    private final String username;
    private final String password;
    private final OkHttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private volatile String cachedToken;
    private volatile Instant tokenExpiry;

    @Autowired
    public OAuth2JwtTokenProvider(
            @Value("${bridge.security.token-url:}") String tokenUrl,
            @Value("${bridge.security.client-id:}") String clientId,
            @Value("${bridge.security.client-secret:}") String clientSecret,
            @Value("${bridge.security.scope:}") String scope,
            @Value("${bridge.security.username:}") String username,
            @Value("${bridge.security.password:}") String password) {
        this.tokenUrl = tokenUrl;
        this.clientId = clientId;
        this.clientSecret = clientSecret;
        this.scope = scope;
        this.username = username;
        this.password = password;
        this.httpClient = new OkHttpClient.Builder()
                .connectTimeout(10, TimeUnit.SECONDS)
                .readTimeout(30, TimeUnit.SECONDS)
                .writeTimeout(30, TimeUnit.SECONDS)
                .build();
        this.objectMapper = new ObjectMapper();
    }

    public OAuth2JwtTokenProvider(
            String tokenUrl,
            String clientId,
            String clientSecret,
            String scope,
            String username,
            String password,
            OkHttpClient httpClient) {
        this.tokenUrl = tokenUrl;
        this.clientId = clientId;
        this.clientSecret = clientSecret;
        this.scope = scope;
        this.username = username;
        this.password = password;
        this.httpClient = httpClient;
        this.objectMapper = new ObjectMapper();
    }

    @PostConstruct
    public void init() {
        logger.info("=== OAuth2 JWT Provider Configuration ===");
        logger.info("  token-url: {}", hasValue(tokenUrl) ? tokenUrl : "(not set)");
        logger.info("  client-id: {}", hasValue(clientId) ? clientId : "(not set)");
        logger.info("  client-secret: {}", hasValue(clientSecret) ? "********" : "(not set)");
        logger.info("  username: {}", hasValue(username) ? username : "(not set)");
        logger.info("  password: {}", hasValue(password) ? "********" : "(not set)");
        logger.info("  scope: {}", hasValue(scope) ? scope : "(not set)");
        logger.info("==========================================");

        if (!hasValue(tokenUrl)) {
            logger.warn("OAuth2 JWT provider token URL not configured");
        }
    }

    @Override
    public String getToken() {
        lock.readLock().lock();
        try {
            if (isTokenValidInternal()) {
                return cachedToken;
            }
        } finally {
            lock.readLock().unlock();
        }

        lock.writeLock().lock();
        try {
            if (isTokenValidInternal()) {
                return cachedToken;
            }
            refreshTokenInternal();
            return cachedToken;
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void refreshToken() {
        lock.writeLock().lock();
        try {
            refreshTokenInternal();
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public boolean isTokenValid() {
        lock.readLock().lock();
        try {
            return isTokenValidInternal();
        } finally {
            lock.readLock().unlock();
        }
    }

    private boolean isTokenValidInternal() {
        return cachedToken != null &&
                tokenExpiry != null &&
                Instant.now().plusSeconds(EXPIRY_BUFFER_SECONDS).isBefore(tokenExpiry);
    }

    private void refreshTokenInternal() {
        if (!hasValue(tokenUrl)) {
            throw new TokenRefreshException("Token URL not configured");
        }

        logger.debug("Refreshing OAuth2 JWT token");

        // The STS (sts/v5/jwt_token_internal) is not a standard OAuth2 form endpoint:
        // client credentials and scope travel as HEADERS, and the body is a JSON
        // document carrying only the username/password. Sending form-encoding gets 415.
        ObjectNode bodyNode = objectMapper.createObjectNode();
        if (hasValue(username)) {
            bodyNode.put("username", username);
        }
        if (hasValue(password)) {
            bodyNode.put("password", password);
        }

        RequestBody body;
        try {
            body = RequestBody.create(
                    objectMapper.writeValueAsString(bodyNode),
                    MediaType.get("application/json"));
        } catch (IOException e) {
            throw new TokenRefreshException("Failed to serialize token request body", e);
        }

        Request.Builder requestBuilder = new Request.Builder()
                .url(tokenUrl)
                .post(body)
                .header("Content-Type", "application/json");

        if (hasValue(clientId)) {
            requestBuilder.header("ClientID", clientId);
        }
        if (hasValue(clientSecret)) {
            requestBuilder.header("ClientSecret", clientSecret);
        }
        if (hasValue(scope)) {
            requestBuilder.header("scope", scope);
        }

        Request request = requestBuilder.build();

        try (Response response = httpClient.newCall(request).execute()) {
            // OkHttp response bodies can only be consumed once — read exactly once, then branch
            String responseBody = response.body() != null ? response.body().string() : "";

            if (!response.isSuccessful()) {
                logger.error("Token refresh failed with status {}: {}", response.code(), responseBody);
                throw new TokenRefreshException("Token refresh failed with status: " + response.code());
            }

            JsonNode json = objectMapper.readTree(responseBody);

            String token = extractToken(json);
            if (token == null) {
                // Log field NAMES only — values could include the token itself
                throw new TokenRefreshException(
                        "Token response has no recognized token field; response fields: "
                                + fieldNames(json));
            }

            cachedToken = token;
            tokenExpiry = extractExpiry(json, token);

            logger.info("OAuth2 JWT token refreshed successfully, expires at: {}", tokenExpiry);

        } catch (IOException e) {
            throw new TokenRefreshException("Failed to refresh token: " + e.getMessage(), e);
        }
    }

    /**
     * The STS response shape is not the standard OAuth2 one; accept the common
     * variants of the token field name.
     */
    private String extractToken(JsonNode json) {
        for (String field : new String[]{"access_token", "accessToken", "token", "jwt", "id_token"}) {
            JsonNode node = json.get(field);
            if (node != null && node.isTextual() && !node.asText().isEmpty()) {
                String token = node.asText();
                // Some STS implementations return the value already prefixed for the
                // Authorization header; sending "Bearer Bearer <jwt>" yields 401
                if (token.startsWith("Bearer ")) {
                    token = token.substring("Bearer ".length());
                }
                logger.info("Token extracted from response field '{}' ({} chars, {} JWT segments); "
                                + "other fields present: {}",
                        field, token.length(), token.split("\\.").length, fieldNames(json));
                return token;
            }
        }
        return null;
    }

    /**
     * Expiry precedence: an explicit expires_in/expiresIn field, then the JWT's own
     * exp claim, then a conservative one-hour default.
     */
    private Instant extractExpiry(JsonNode json, String token) {
        for (String field : new String[]{"expires_in", "expiresIn"}) {
            JsonNode node = json.get(field);
            if (node != null && node.isNumber()) {
                return Instant.now().plusSeconds(node.asLong());
            }
        }

        Instant jwtExpiry = expiryFromJwt(token);
        if (jwtExpiry != null) {
            return jwtExpiry;
        }

        return Instant.now().plusSeconds(3600);
    }

    private Instant expiryFromJwt(String token) {
        try {
            String[] parts = token.split("\\.");
            if (parts.length < 2) {
                return null;
            }
            byte[] payload = Base64.getUrlDecoder().decode(parts[1]);
            JsonNode claims = objectMapper.readTree(payload);
            JsonNode exp = claims.get("exp");
            return exp != null && exp.isNumber() ? Instant.ofEpochSecond(exp.asLong()) : null;
        } catch (Exception e) {
            logger.debug("Could not read exp claim from JWT: {}", e.getMessage());
            return null;
        }
    }

    private String fieldNames(JsonNode json) {
        StringBuilder sb = new StringBuilder();
        json.fieldNames().forEachRemaining(name -> {
            if (sb.length() > 0) {
                sb.append(", ");
            }
            sb.append(name);
        });
        return sb.length() > 0 ? sb.toString() : "<none>";
    }

    private boolean hasValue(String value) {
        return value != null && !value.isEmpty();
    }

    public static class TokenRefreshException extends RuntimeException {
        public TokenRefreshException(String message) {
            super(message);
        }

        public TokenRefreshException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
