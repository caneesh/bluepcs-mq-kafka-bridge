package com.hcsc.bridge.diagnostics;

import java.util.Collections;
import java.util.List;

/** Every probe's result for one validate-only run. Passing means no probe FAILED. */
public final class ReadinessReport {

    private final List<CheckResult> results;

    public ReadinessReport(List<CheckResult> results) {
        this.results = Collections.unmodifiableList(results);
    }

    public List<CheckResult> getResults() {
        return results;
    }

    public boolean isPassed() {
        return results.stream().noneMatch(CheckResult::isFailed);
    }

    public long getPassedCount() {
        return results.stream().filter(CheckResult::isPassed).count();
    }

    public long getFailedCount() {
        return results.stream().filter(CheckResult::isFailed).count();
    }

    public long getSkippedCount() {
        return results.stream().filter(CheckResult::isSkipped).count();
    }
}
