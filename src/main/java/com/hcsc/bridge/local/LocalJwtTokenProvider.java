package com.hcsc.bridge.local;

import com.hcsc.bridge.security.JwtTokenProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component
@Profile("local")
public class LocalJwtTokenProvider implements JwtTokenProvider {

    private static final Logger logger = LoggerFactory.getLogger(LocalJwtTokenProvider.class);
    private static final String LOCAL_TOKEN = "local-dev-token";

    @Override
    public String getToken() {
        logger.debug("Returning local development token");
        return LOCAL_TOKEN;
    }

    @Override
    public void refreshToken() {
        logger.debug("Token refresh called (no-op in local mode)");
    }

    @Override
    public boolean isTokenValid() {
        return true;
    }
}
