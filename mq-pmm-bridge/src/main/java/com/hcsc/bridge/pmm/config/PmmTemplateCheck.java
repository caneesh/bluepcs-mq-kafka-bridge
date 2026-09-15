package com.hcsc.bridge.pmm.config;

import com.hcsc.bridge.diagnostics.CheckResult;
import com.hcsc.bridge.diagnostics.ReadinessCheck;
import com.hcsc.bridge.pmm.template.PmmRequestTemplate;
import com.hcsc.bridge.pmm.xml.PmmExtractedValues;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

/**
 * Proves the configured request template renders: it loads, both placeholders substitute,
 * and no dollar-brace token survives into the body that would be POSTed. Catches a template
 * an operator pointed at but never exercised, before a message does.
 */
@Component
@Order(60)
public class PmmTemplateCheck implements ReadinessCheck {

    private static final Logger logger = LoggerFactory.getLogger(PmmTemplateCheck.class);

    private final ObjectProvider<PmmRequestTemplate> templateProvider;

    public PmmTemplateCheck(ObjectProvider<PmmRequestTemplate> templateProvider) {
        this.templateProvider = templateProvider;
    }

    @Override
    public String name() {
        return "PMM_TEMPLATE";
    }

    @Override
    public CheckResult run() {
        PmmRequestTemplate template = templateProvider.getIfAvailable();
        if (template == null) {
            return CheckResult.fail(name(), "request template bean not available (see startup errors)");
        }
        try {
            String rendered = template.render(new PmmExtractedValues("probe-value-1", "probe-value-2"));
            if (rendered.contains("${")) {
                return CheckResult.fail(name(), "rendered template still contains a placeholder");
            }
            String message = String.format("template %s renders (%d chars)",
                    template.getLocation(), rendered.length());
            logger.info("[PASS] {}: {}", name(), message);
            return CheckResult.pass(name(), message);
        } catch (RuntimeException e) {
            String message = "template render failed: " + e.getMessage();
            logger.error("[FAIL] {}: {}", name(), message);
            return CheckResult.fail(name(), message);
        }
    }
}
