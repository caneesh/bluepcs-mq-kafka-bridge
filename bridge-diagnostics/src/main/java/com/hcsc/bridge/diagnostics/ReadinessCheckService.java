package com.hcsc.bridge.diagnostics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

/**
 * Runs the readiness probes an application has composed and reports the outcome.
 *
 * <p>It holds no probe logic of its own: every {@link ReadinessCheck} bean on the
 * classpath is run, in the order Spring supplies them. That is how each application
 * selects what "ready" means for it - the PMM bridge publishes checks for its web service
 * and request template, and they appear here without this class knowing about them, and
 * without an application replacing the service to add one.
 */
@Service
public class ReadinessCheckService {

    private static final Logger logger = LoggerFactory.getLogger(ReadinessCheckService.class);

    private final List<ReadinessCheck> checks;

    public ReadinessCheckService(List<ReadinessCheck> checks) {
        this.checks = checks;
    }

    public ReadinessReport runAllChecks() {
        logger.info("=== RUNNING READINESS CHECKS ({}) ===", checks.size());

        List<CheckResult> results = new ArrayList<>();
        for (ReadinessCheck check : checks) {
            logger.info("Checking {}...", check.name());
            try {
                results.add(check.run());
            } catch (RuntimeException e) {
                // A probe must not throw, but a broken one must not hide the others either.
                String message = "check threw " + e.getClass().getSimpleName() + ": " + e.getMessage();
                logger.error("[FAIL] {}: {}", check.name(), message);
                results.add(CheckResult.fail(check.name(), message));
            }
        }

        ReadinessReport report = new ReadinessReport(results);
        logger.info("=== VALIDATION RESULT: {} ===", report.isPassed() ? "PASSED" : "FAILED");
        return report;
    }
}
