package com.hcsc.bridge.diagnostics;

/**
 * The outcome of one readiness probe. PASS and FAIL are self-explanatory; SKIP means the
 * probe had nothing to check (the integration is not configured in this profile), which is
 * deliberately not a failure - validate-only mode must be usable on a partially configured
 * environment without reporting false problems.
 */
public final class CheckResult {

    private final String name;
    private final Status status;
    private final String message;

    public enum Status {
        PASS, FAIL, SKIP
    }

    private CheckResult(String name, Status status, String message) {
        this.name = name;
        this.status = status;
        this.message = message;
    }

    public static CheckResult pass(String name, String message) {
        return new CheckResult(name, Status.PASS, message);
    }

    public static CheckResult fail(String name, String message) {
        return new CheckResult(name, Status.FAIL, message);
    }

    public static CheckResult skip(String name, String message) {
        return new CheckResult(name, Status.SKIP, message);
    }

    public String getName() {
        return name;
    }

    public Status getStatus() {
        return status;
    }

    public String getMessage() {
        return message;
    }

    public boolean isPassed() {
        return status == Status.PASS;
    }

    public boolean isFailed() {
        return status == Status.FAIL;
    }

    public boolean isSkipped() {
        return status == Status.SKIP;
    }

    @Override
    public String toString() {
        return "[" + status + "] " + name + ": " + message;
    }
}
