package com.hcsc.bridge.security;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.FormBody;
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

        FormBody.Builder formBuilder = new FormBody.Builder();

        if (hasValue(clientId)) {
            formBuilder.add("client_id", clientId);
        }

        if (hasValue(clientSecret)) {
            formBuilder.add("client_secret", clientSecret);
        }

        if (hasValue(username)) {
            formBuilder.add("username", username);
        }

        if (hasValue(password)) {
            formBuilder.add("password", password);
        }

        if (hasValue(scope)) {
            formBuilder.add("scope", scope);
        }

        RequestBody body = formBuilder.build();

        Request request = new Request.Builder()
                .url(tokenUrl)
                .post(body)
                .header("Content-Type", "application/x-www-form-urlencoded")
                .build();

        try (Response response = httpClient.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                String errorBody = response.body() != null ? response.body().string() : "";
                logger.error("Token refresh failed with status {}: {}", response.code(), errorBody);
                throw new TokenRefreshException("Token refresh failed with status: " + response.code());
            }

            String responseBody = response.body() != null ? response.body().string() : "";
            JsonNode json = objectMapper.readTree(responseBody);

            if (!json.has("access_token")) {
                throw new TokenRefreshException("Token response missing access_token");
            }

            cachedToken = json.get("access_token").asText();
            long expiresIn = json.has("expires_in") ? json.get("expires_in").asLong() : 3600;
            tokenExpiry = Instant.now().plusSeconds(expiresIn);

            logger.info("OAuth2 JWT token refreshed successfully, expires at: {}", tokenExpiry);

        } catch (IOException e) {
            throw new TokenRefreshException("Failed to refresh token: " + e.getMessage(), e);
        }
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
