package com.hcsc.bridge.pmm.config;

import com.hcsc.bridge.diagnostics.CheckResult;
import com.hcsc.bridge.diagnostics.ReadinessCheck;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;

/**
 * Proves the PMM web service host accepts a connection. A readiness check the PMM bridge
 * contributes to the shared runner: publishing this bean is the whole of "the PMM bridge
 * also checks its web service", with no subclassing or bean replacement involved.
 */
@Component
@Order(50)
public class PmmApiReachableCheck implements ReadinessCheck {

    private static final Logger logger = LoggerFactory.getLogger(PmmApiReachableCheck.class);

    private final String apiUrl;

    public PmmApiReachableCheck(@Value("${bridge.pmm.api.url:}") String apiUrl) {
        this.apiUrl = apiUrl;
    }

    @Override
    public String name() {
        return "PMM_API_REACHABLE";
    }

    @Override
    public CheckResult run() {
        if (apiUrl == null || apiUrl.trim().isEmpty()) {
            return CheckResult.skip(name(), "bridge.pmm.api.url not configured");
        }
        try {
            URI uri = URI.create(apiUrl.trim());
            String host = uri.getHost();
            int port = uri.getPort() > 0 ? uri.getPort() : ("https".equalsIgnoreCase(uri.getScheme()) ? 443 : 80);
            if (host == null) {
                return CheckResult.fail(name(), "bridge.pmm.api.url has no host: " + apiUrl);
            }
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress(host, port), 5000);
            }
            String message = String.format("PMM API host reachable at %s:%d", host, port);
            logger.info("[PASS] {}: {}", name(), message);
            return CheckResult.pass(name(), message);
        } catch (Exception e) {
            String message = String.format("Cannot reach PMM API %s - %s", apiUrl, e.getMessage());
            logger.error("[FAIL] {}: {}", name(), message);
            return CheckResult.fail(name(), message);
        }
    }
}
