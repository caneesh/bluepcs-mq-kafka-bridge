package com.hcsc.bridge.pmm.config;

import com.hcsc.bridge.config.ReadinessCheckService;
import com.hcsc.bridge.pmm.template.PmmRequestTemplate;
import com.hcsc.bridge.pmm.xml.PmmExtractedValues;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Service;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * Validate-only checks for the PMM bridge: the shared MQ / Kafka / HDFS / STS probes from
 * bridge-core plus reachability of the PMM web service and a dry render of the request
 * template. {@code @Primary} so the core {@code ValidateOnlyRunner} picks this one.
 */
@Service
@Primary
public class PmmReadinessCheckService extends ReadinessCheckService {

    private static final Logger logger = LoggerFactory.getLogger(PmmReadinessCheckService.class);

    private final ObjectProvider<PmmRequestTemplate> templateProvider;
    private final String apiUrl;

    public PmmReadinessCheckService(ObjectProvider<Configuration> hadoopConfigurationProvider,
                                    ObjectProvider<PmmRequestTemplate> templateProvider,
                                    @Value("${bridge.pmm.api.url:}") String apiUrl) {
        super(hadoopConfigurationProvider);
        this.templateProvider = templateProvider;
        this.apiUrl = apiUrl;
    }

    @Override
    public ReadinessReport runAllChecks() {
        ReadinessReport base = super.runAllChecks();
        List<CheckResult> results = new ArrayList<>(base.getResults());
        results.add(checkApiReachable());
        results.add(checkTemplate());
        ReadinessReport report = new ReadinessReport(results);
        logger.info("=== PMM VALIDATION RESULT: {} ===", report.isPassed() ? "PASSED" : "FAILED");
        return report;
    }

    private CheckResult checkApiReachable() {
        String name = "PMM_API_REACHABLE";
        if (apiUrl == null || apiUrl.trim().isEmpty()) {
            return CheckResult.skip(name, "bridge.pmm.api.url not configured");
        }
        try {
            URI uri = URI.create(apiUrl.trim());
            String host = uri.getHost();
            int port = uri.getPort() > 0 ? uri.getPort() : ("https".equalsIgnoreCase(uri.getScheme()) ? 443 : 80);
            if (host == null) {
                return CheckResult.fail(name, "bridge.pmm.api.url has no host: " + apiUrl);
            }
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress(host, port), 5000);
            }
            String message = String.format("PMM API host reachable at %s:%d", host, port);
            logger.info("[PASS] {}: {}", name, message);
            return CheckResult.pass(name, message);
        } catch (Exception e) {
            String message = String.format("Cannot reach PMM API %s - %s", apiUrl, e.getMessage());
            logger.error("[FAIL] {}: {}", name, message);
            return CheckResult.fail(name, message);
        }
    }

    private CheckResult checkTemplate() {
        String name = "PMM_TEMPLATE";
        PmmRequestTemplate template = templateProvider.getIfAvailable();
        if (template == null) {
            return CheckResult.fail(name, "request template bean not available (see startup errors)");
        }
        try {
            String rendered = template.render(new PmmExtractedValues("probe-value-1", "probe-value-2"));
            if (rendered.contains("${")) {
                return CheckResult.fail(name, "rendered template still contains a placeholder");
            }
            String message = String.format("template %s renders (%d chars)", template.getLocation(), rendered.length());
            logger.info("[PASS] {}: {}", name, message);
            return CheckResult.pass(name, message);
        } catch (RuntimeException e) {
            String message = "template render failed: " + e.getMessage();
            logger.error("[FAIL] {}: {}", name, message);
            return CheckResult.fail(name, message);
        }
    }
}
