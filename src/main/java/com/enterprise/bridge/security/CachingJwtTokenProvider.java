package com.enterprise.bridge.security;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Component
public class CachingJwtTokenProvider implements JwtTokenProvider {

    private static final Logger logger = LoggerFactory.getLogger(CachingJwtTokenProvider.class);
    private static final long EXPIRY_BUFFER_SECONDS = 60;

    private final RestTemplate restTemplate;
    private final String tokenUrl;
    private final String clientId;
    private final String clientSecret;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private volatile String cachedToken;
    private volatile Instant tokenExpiry;

    public CachingJwtTokenProvider(
            RestTemplate restTemplate,
            @Value("${bridge.security.token-url:}") String tokenUrl,
            @Value("${bridge.security.client-id:}") String clientId,
            @Value("${bridge.security.client-secret:}") String clientSecret) {
        this.restTemplate = restTemplate;
        this.tokenUrl = tokenUrl;
        this.clientId = clientId;
        this.clientSecret = clientSecret;
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

    @SuppressWarnings("unchecked")
    private void refreshTokenInternal() {
        if (tokenUrl == null || tokenUrl.isEmpty()) {
            cachedToken = "dev-token";
            tokenExpiry = Instant.now().plusSeconds(3600);
            return;
        }

        logger.debug("Refreshing JWT token");

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);

        MultiValueMap<String, String> body = new LinkedMultiValueMap<>();
        body.add("grant_type", "client_credentials");
        body.add("client_id", clientId);
        body.add("client_secret", clientSecret);

        HttpEntity<MultiValueMap<String, String>> request = new HttpEntity<>(body, headers);

        ResponseEntity<Map> response = restTemplate.postForEntity(tokenUrl, request, Map.class);

        if (response.getBody() != null) {
            Map<String, Object> responseBody = response.getBody();
            cachedToken = (String) responseBody.get("access_token");
            Number expiresIn = (Number) responseBody.getOrDefault("expires_in", 3600);
            tokenExpiry = Instant.now().plusSeconds(expiresIn.longValue());
            logger.debug("JWT token refreshed, expires at: {}", tokenExpiry);
        }
    }
}
