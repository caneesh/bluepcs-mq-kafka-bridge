package com.enterprise.bridge.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import java.util.Arrays;

@Component
public class ValidateOnlyRunner implements ApplicationRunner {

    private static final Logger logger = LoggerFactory.getLogger(ValidateOnlyRunner.class);

    static final int EXIT_PASSED = 0;
    static final int EXIT_FAILED = 1;
    static final int EXIT_EXCEPTION = 2;

    @Value("${bridge.validate-only:false}")
    private boolean validateOnly;

    private final ReadinessCheckService readinessCheckService;
    private final ApplicationContext applicationContext;
    private final Environment environment;

    @Autowired
    public ValidateOnlyRunner(ReadinessCheckService readinessCheckService,
                               ApplicationContext applicationContext,
                               Environment environment) {
        this.readinessCheckService = readinessCheckService;
        this.applicationContext = applicationContext;
        this.environment = environment;
    }

    private boolean isTestEnvironment() {
        String[] activeProfiles = environment.getActiveProfiles();
        return Arrays.asList(activeProfiles).contains("test") ||
               System.getProperty("spring.test.context") != null ||
               applicationContext.getEnvironment().getProperty("spring.profiles.active", "").contains("test");
    }

    @Override
    public void run(ApplicationArguments args) {
        if (!validateOnly) {
            logger.debug("Validate-only mode is disabled, continuing normal startup");
            return;
        }

        if (isTestEnvironment()) {
            logger.debug("Validate-only mode skipped in test environment");
            return;
        }

        logger.info("============================================");
        logger.info("RUNNING IN VALIDATE-ONLY MODE");
        logger.info("============================================");

        final int exitCode = computeExitCode();

        logger.info("Exiting with code: {}", exitCode);
        SpringApplication.exit(applicationContext, () -> exitCode);
        System.exit(exitCode);
    }

    public int runValidation() {
        logger.info("============================================");
        logger.info("RUNNING VALIDATION");
        logger.info("============================================");
        return computeExitCode();
    }

    private int computeExitCode() {
        try {
            ReadinessCheckService.ReadinessReport report = readinessCheckService.runAllChecks();

            if (report.isPassed()) {
                logger.info("============================================");
                logger.info("VALIDATION RESULT: PASSED");
                logger.info("  Passed: {}", report.getPassedCount());
                logger.info("  Skipped: {}", report.getSkippedCount());
                logger.info("  Failed: {}", report.getFailedCount());
                logger.info("============================================");
                return EXIT_PASSED;
            } else {
                logger.error("============================================");
                logger.error("VALIDATION RESULT: FAILED");
                logger.error("  Passed: {}", report.getPassedCount());
                logger.error("  Skipped: {}", report.getSkippedCount());
                logger.error("  Failed: {}", report.getFailedCount());
                logger.error("============================================");

                for (ReadinessCheckService.CheckResult result : report.getResults()) {
                    if (result.isFailed()) {
                        logger.error("[FAIL] {}: {}", result.getName(), result.getMessage());
                    }
                }

                return EXIT_FAILED;
            }
        } catch (Exception e) {
            logger.error("============================================");
            logger.error("VALIDATION RESULT: EXCEPTION");
            logger.error("Error during validation: {}", e.getMessage(), e);
            logger.error("============================================");
            return EXIT_EXCEPTION;
        }
    }
}
