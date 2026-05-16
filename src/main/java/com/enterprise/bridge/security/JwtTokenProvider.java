package com.enterprise.bridge.security;

public interface JwtTokenProvider {

    String getToken();

    void refreshToken();

    boolean isTokenValid();
}
