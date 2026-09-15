package com.hcsc.bridge.diagnostics;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("ReadinessCheckService: composing whatever checks an application publishes")
class ReadinessCheckServiceTest {

    private static ReadinessCheck check(String name, CheckResult result) {
        return new ReadinessCheck() {
            @Override
            public String name() {
                return name;
            }

            @Override
            public CheckResult run() {
                return result;
            }
        };
    }

    @Test
    @DisplayName("runs every check it was given and reports each result")
    void runsEveryCheck() {
        ReadinessReport report = new ReadinessCheckService(List.of(
                check("A", CheckResult.pass("A", "ok")),
                check("B", CheckResult.skip("B", "not configured")),
                check("C", CheckResult.pass("C", "ok")))).runAllChecks();

        assertThat(report.getResults()).extracting(CheckResult::getName).containsExactly("A", "B", "C");
        assertThat(report.isPassed()).isTrue();
        assertThat(report.getPassedCount()).isEqualTo(2);
        assertThat(report.getSkippedCount()).isEqualTo(1);
    }

    @Test
    @DisplayName("one failing check fails the report; a skip never does")
    void failureFailsTheReport() {
        ReadinessReport report = new ReadinessCheckService(List.of(
                check("A", CheckResult.pass("A", "ok")),
                check("B", CheckResult.fail("B", "unreachable")))).runAllChecks();

        assertThat(report.isPassed()).isFalse();
        assertThat(report.getFailedCount()).isEqualTo(1);
    }

    @Test
    @DisplayName("a check that throws is reported as failed without hiding the others")
    void throwingCheckIsContained() {
        ReadinessCheck broken = new ReadinessCheck() {
            @Override
            public String name() {
                return "BROKEN";
            }

            @Override
            public CheckResult run() {
                throw new IllegalStateException("probe bug");
            }
        };

        ReadinessReport report = new ReadinessCheckService(List.of(
                broken, check("AFTER", CheckResult.pass("AFTER", "ok")))).runAllChecks();

        assertThat(report.getResults()).extracting(CheckResult::getName).containsExactly("BROKEN", "AFTER");
        assertThat(report.getResults().get(0).isFailed()).isTrue();
        assertThat(report.getResults().get(0).getMessage()).contains("IllegalStateException").contains("probe bug");
        assertThat(report.getResults().get(1).isPassed()).isTrue();
    }

    @Test
    @DisplayName("an application that publishes no checks gets an empty, passing report")
    void noChecks() {
        ReadinessReport report = new ReadinessCheckService(List.of()).runAllChecks();

        assertThat(report.getResults()).isEmpty();
        assertThat(report.isPassed()).isTrue();
    }
}
