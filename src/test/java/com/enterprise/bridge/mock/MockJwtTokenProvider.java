package com.enterprise.bridge.mock;

import com.enterprise.bridge.security.JwtTokenProvider;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class MockJwtTokenProvider implements JwtTokenProvider {

    private static final String DEFAULT_TOKEN = "mock-jwt-token-12345";

    private String token = DEFAULT_TOKEN;
    private final AtomicBoolean tokenValid = new AtomicBoolean(true);
    private final AtomicInteger refreshCount = new AtomicInteger(0);
    private final AtomicInteger tokenRequestCount = new AtomicInteger(0);

    @Override
    public String getToken() {
        tokenRequestCount.incrementAndGet();
        return token;
    }

    @Override
    public void refreshToken() {
        refreshCount.incrementAndGet();
        token = "refreshed-mock-jwt-token-" + System.currentTimeMillis();
        tokenValid.set(true);
    }

    @Override
    public boolean isTokenValid() {
        return tokenValid.get();
    }

    public void setToken(String token) {
        this.token = token;
    }

    public void setTokenValid(boolean valid) {
        this.tokenValid.set(valid);
    }

    public int getRefreshCount() {
        return refreshCount.get();
    }

    public int getTokenRequestCount() {
        return tokenRequestCount.get();
    }

    public void reset() {
        token = DEFAULT_TOKEN;
        tokenValid.set(true);
        refreshCount.set(0);
        tokenRequestCount.set(0);
    }
}
