package com.enterprise.bridge.security;

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
            @Value("${bridge.security.scope:}") String scope) {
        this.tokenUrl = tokenUrl;
        this.clientId = clientId;
        this.clientSecret = clientSecret;
        this.scope = scope;
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
            OkHttpClient httpClient) {
        this.tokenUrl = tokenUrl;
        this.clientId = clientId;
        this.clientSecret = clientSecret;
        this.scope = scope;
        this.httpClient = httpClient;
        this.objectMapper = new ObjectMapper();
    }

    @PostConstruct
    public void init() {
        if (tokenUrl != null && !tokenUrl.isEmpty()) {
            logger.info("OAuth2 JWT provider configured with token URL (credentials not logged)");
        } else {
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
        if (tokenUrl == null || tokenUrl.isEmpty()) {
            throw new TokenRefreshException("Token URL not configured");
        }

        logger.debug("Refreshing OAuth2 JWT token");

        FormBody.Builder formBuilder = new FormBody.Builder()
                .add("grant_type", "client_credentials")
                .add("client_id", clientId)
                .add("client_secret", clientSecret);

        if (scope != null && !scope.isEmpty()) {
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

            logger.debug("OAuth2 JWT token refreshed, expires at: {}", tokenExpiry);

        } catch (IOException e) {
            throw new TokenRefreshException("Failed to refresh token", e);
        }
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
