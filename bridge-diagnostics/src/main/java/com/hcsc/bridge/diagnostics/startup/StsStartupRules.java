package com.hcsc.bridge.diagnostics.startup;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;

/**
 * The STS credential rules both bridges enforce at startup. Only the token endpoint may
 * differ between them; the requirement that a client id and secret are present does not.
 */
public final class StsStartupRules {

    private static final Logger logger = LoggerFactory.getLogger(StsStartupRules.class);

    private final String oauthTokenUrl;
    private final String oauthClientId;
    private final String oauthClientSecret;

    public StsStartupRules(String oauthTokenUrl, String oauthClientId, String oauthClientSecret) {
        this.oauthTokenUrl = oauthTokenUrl;
        this.oauthClientId = oauthClientId;
        this.oauthClientSecret = oauthClientSecret;
    }

    public void validateOAuthConfig(List<String> errors, List<String> warnings) {
        logger.info("Validating OAuth configuration...");

        if (isBlank(oauthTokenUrl)) {
            errors.add("[OAUTH] bridge.security.token-url is required");
        } else {
            if (!oauthTokenUrl.startsWith("http://") && !oauthTokenUrl.startsWith("https://")) {
                errors.add("[OAUTH] bridge.security.token-url must start with http:// or https://");
            } else {
                logger.info("[OAUTH] Token URL: {}", oauthTokenUrl);
            }
        }

        if (isBlank(oauthClientId)) {
            errors.add("[OAUTH] bridge.security.client-id is required");
        } else {
            logger.info("[OAUTH] Client ID: {}", oauthClientId);
        }

        if (isBlank(oauthClientSecret)) {
            errors.add("[OAUTH] bridge.security.client-secret is required (env: OAUTH_CLIENT_SECRET)");
        } else {
            logger.info("[OAUTH] Client Secret: ********");
        }
    }
    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }
}
