package com.enterprise.bridge.config;

import org.junit.jupiter.api.Test;
import org.springframework.boot.ApplicationArguments;
import org.springframework.context.ApplicationContext;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class ValidateOnlyRunnerTest {

    @Test
    void run_whenValidateOnlyDisabled_shouldContinueNormally() throws Exception {
        ReadinessCheckService mockReadinessService = mock(ReadinessCheckService.class);
        ApplicationContext mockContext = mock(ApplicationContext.class);
        org.springframework.core.env.Environment mockEnvironment = mock(org.springframework.core.env.Environment.class);
        ApplicationArguments mockArgs = mock(ApplicationArguments.class);

        when(mockEnvironment.getActiveProfiles()).thenReturn(new String[]{});

        ValidateOnlyRunner runner = new ValidateOnlyRunner(mockReadinessService, mockContext, mockEnvironment);

        // Use reflection to set validateOnly = false
        java.lang.reflect.Field validateOnlyField = ValidateOnlyRunner.class.getDeclaredField("validateOnly");
        validateOnlyField.setAccessible(true);
        validateOnlyField.set(runner, false);

        // Should not throw and should not call readiness checks
        runner.run(mockArgs);

        verify(mockReadinessService, never()).runAllChecks();
    }

    @Test
    void readinessCheckService_shouldReturnReport() {
        ReadinessCheckService service = new ReadinessCheckService();

        // This will run checks but connections will fail (expected in unit test)
        ReadinessCheckService.ReadinessReport report = service.runAllChecks();

        assertNotNull(report);
        assertNotNull(report.getResults());
        assertFalse(report.getResults().isEmpty());
    }

    @Test
    void checkResult_passShouldHaveCorrectStatus() {
        ReadinessCheckService.CheckResult result = ReadinessCheckService.CheckResult.pass("TEST", "passed");

        assertEquals("TEST", result.getName());
        assertEquals(ReadinessCheckService.CheckResult.Status.PASS, result.getStatus());
        assertEquals("passed", result.getMessage());
        assertTrue(result.isPassed());
        assertFalse(result.isFailed());
        assertFalse(result.isSkipped());
    }

    @Test
    void checkResult_failShouldHaveCorrectStatus() {
        ReadinessCheckService.CheckResult result = ReadinessCheckService.CheckResult.fail("TEST", "failed");

        assertEquals("TEST", result.getName());
        assertEquals(ReadinessCheckService.CheckResult.Status.FAIL, result.getStatus());
        assertEquals("failed", result.getMessage());
        assertFalse(result.isPassed());
        assertTrue(result.isFailed());
        assertFalse(result.isSkipped());
    }

    @Test
    void checkResult_skipShouldHaveCorrectStatus() {
        ReadinessCheckService.CheckResult result = ReadinessCheckService.CheckResult.skip("TEST", "skipped");

        assertEquals("TEST", result.getName());
        assertEquals(ReadinessCheckService.CheckResult.Status.SKIP, result.getStatus());
        assertEquals("skipped", result.getMessage());
        assertFalse(result.isPassed());
        assertFalse(result.isFailed());
        assertTrue(result.isSkipped());
    }

    @Test
    void readinessReport_shouldCalculateCorrectCounts() {
        java.util.List<ReadinessCheckService.CheckResult> results = new java.util.ArrayList<>();
        results.add(ReadinessCheckService.CheckResult.pass("A", "passed"));
        results.add(ReadinessCheckService.CheckResult.pass("B", "passed"));
        results.add(ReadinessCheckService.CheckResult.fail("C", "failed"));
        results.add(ReadinessCheckService.CheckResult.skip("D", "skipped"));

        ReadinessCheckService.ReadinessReport report = new ReadinessCheckService.ReadinessReport(results);

        assertEquals(2, report.getPassedCount());
        assertEquals(1, report.getFailedCount());
        assertEquals(1, report.getSkippedCount());
        assertFalse(report.isPassed()); // Has a failure
    }

    @Test
    void readinessReport_shouldPassWithNoFailures() {
        java.util.List<ReadinessCheckService.CheckResult> results = new java.util.ArrayList<>();
        results.add(ReadinessCheckService.CheckResult.pass("A", "passed"));
        results.add(ReadinessCheckService.CheckResult.skip("B", "skipped"));

        ReadinessCheckService.ReadinessReport report = new ReadinessCheckService.ReadinessReport(results);

        assertTrue(report.isPassed());
    }
}
