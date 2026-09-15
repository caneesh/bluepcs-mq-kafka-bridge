package com.hcsc.bridge.diagnostics;

/**
 * One thing validate-only mode proves before an operator enables consumption: that MQ
 * answers, that HDFS is reachable, that the STS issues a usable token, and so on.
 *
 * <p>Checks are beans, and {@link ReadinessCheckService} runs whichever ones are on the
 * application's classpath. An application adds a probe for its own integration by
 * publishing another bean of this type - it does not subclass or replace the service, so
 * "which checks run" is visible in the module that owns each check rather than hidden in
 * an override.
 */
public interface ReadinessCheck {

    /** Stable name for the sysout line and the scheduler's log; also used in the report. */
    String name();

    /** Runs the probe. Must not throw: return {@link CheckResult#fail} instead. */
    CheckResult run();
}
