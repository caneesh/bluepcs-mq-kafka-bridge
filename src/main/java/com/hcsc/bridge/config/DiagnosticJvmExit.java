package com.hcsc.bridge.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;

/**
 * Terminates a short-lived diagnostic JVM (monitor, validate-only, component-test,
 * replay) with a hard upper bound on how long shutdown may take.
 *
 * <p>Why this exists: these modes end with {@code SpringApplication.exit(context, ...)}
 * followed by {@code System.exit()}. Both steps can block indefinitely — the context
 * close runs bean destruction (the Hadoop {@code FileSystem}, the lazily created Kafka
 * {@code AdminClient}), and {@code System.exit} runs shutdown hooks. A single blocked
 * dependency there leaves the JVM alive forever with its checks already finished.
 *
 * <p>That is not hypothetical: on a test edge node, {@code monitor.sh} running every
 * ~10 minutes from Control-M accumulated ~24 live JVMs over 25 days, each still holding
 * memory, file descriptors and HDFS/Kafka connections. They also broke supervision,
 * because they run the same jar as the bridge and so were counted as bridge processes.
 *
 * <p>A daemon watchdog therefore halts the JVM if the orderly path has not completed in
 * {@link #SHUTDOWN_GRACE_MILLIS}. {@code Runtime.halt} is deliberate: it skips shutdown
 * hooks, which is the one way out when a hook itself is what is stuck. The exit code is
 * preserved either way, so schedulers still route on it.
 */
final class DiagnosticJvmExit {

    private static final Logger logger = LoggerFactory.getLogger(DiagnosticJvmExit.class);

    /** Generous next to any real shutdown, short next to a Control-M cycle. */
    static final long SHUTDOWN_GRACE_MILLIS = 30_000L;

    private DiagnosticJvmExit() {
    }

    static void exit(ApplicationContext applicationContext, int exitCode) {
        startWatchdog(exitCode);
        try {
            SpringApplication.exit(applicationContext, () -> exitCode);
        } catch (RuntimeException e) {
            // Never let a shutdown failure become the JVM's fate: the checks already
            // produced a verdict and the scheduler needs that exit code.
            logger.error("Context shutdown failed; exiting anyway with code {}", exitCode, e);
        }
        System.exit(exitCode);
    }

    private static void startWatchdog(int exitCode) {
        Thread watchdog = new Thread(() -> {
            try {
                Thread.sleep(SHUTDOWN_GRACE_MILLIS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            logger.error("Shutdown did not finish within {} ms — halting the JVM with code {}. "
                            + "A dependency's close() is stuck (HDFS or Kafka client); the checks "
                            + "themselves already completed.",
                    SHUTDOWN_GRACE_MILLIS, exitCode);
            Runtime.getRuntime().halt(exitCode);
        }, "diagnostic-exit-watchdog");
        // Daemon so it can never be the reason the JVM stays up, yet it keeps running
        // during shutdown — which is exactly when it needs to fire.
        watchdog.setDaemon(true);
        watchdog.start();
    }
}
