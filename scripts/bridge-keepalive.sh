#!/bin/bash
# =============================================================================
# Keep-alive for the MQ-Kafka Bridge — no-sudo interim supervision.
# =============================================================================
# For environments where systemd units cannot be installed (service account
# not in sudoers). Schedule as a Control-M CYCLIC job every ~5 minutes on the
# edge-node agent, Run As the service account (or from the account's crontab).
#
# Each run does ONE of:
#   - bridge healthy                          -> exit 0, no action
#   - pid file stale/lost but a bridge
#     process is running                      -> adopt it (re-point pid file,
#                                                reset failure counter), then
#                                                health-check as normal
#   - bridge process dead                     -> start it (detached), exit 0
#   - bridge alive but unhealthy N runs in a
#     row (default 3)                         -> kill it, start fresh, exit 0
#   - health URL answers but no bridge
#     process found (foreign process on the
#     port)                                   -> exit 1, do NOT start a duplicate
#   - MULTIPLE bridge processes found         -> exit 1, manual triage (never
#                                                guess which one to supervise)
#   - kill -9 did not reap the old process    -> exit 1, do NOT start a duplicate
#   - start attempted but process not up      -> exit 1 (turn this red in
#                                                Control-M and alert)
#
# Concurrent runs (cyclic job + manual invocation) are serialized by an flock
# on .keepalive.lock — the second run exits 0 immediately.
#
# Output markers for Control-M On-Do text matching:
#   "KEEPALIVE: STARTED"    - bridge was (re)started this run
#   "KEEPALIVE: ADOPTED"    - running bridge found with stale/missing pid file;
#                             pid file re-pointed, no restart (notify, not page)
#   "KEEPALIVE: START-FAIL" - start attempt failed, or refused to start:
#                             foreign process on the health port, multiple
#                             bridge processes, or old pid survived kill -9
#
# DECOMMISSION this job once the systemd units (deploy/*.service) are
# installed — two supervisors restarting the same process will fight.
#
# Unlike the watchdog (which only acts on a service systemd knows about), this
# script cannot tell "intentionally stopped" from "dead" — to stop the bridge
# on purpose, hold/disable the Control-M job first, then kill the process.
# =============================================================================

set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# Load .env FIRST (same convention as the other scripts): it supplies
# BRIDGE_PROFILE plus optional tuning (HEALTH_URL, MAX_FAILURES, ...), so the
# same Control-M job definition works unchanged in test and prod.
if [ -f "${PROJECT_DIR}/.env" ]; then
    set -a
    # shellcheck disable=SC1091
    source "${PROJECT_DIR}/.env"
    set +a
fi

# Profile: explicit argument > BRIDGE_PROFILE from .env > test-env (safe default)
PROFILE="${1:-${BRIDGE_PROFILE:-test-env}}"
# Liveness group, NOT the full aggregate endpoint: a Kafka/HDFS outage takes the
# aggregate DOWN, and restarting a healthy bridge every cycle for the duration of a
# broker incident is exactly the churn a supervisor must not cause. The liveness
# group (mqListener,ping) is DOWN only when the bridge process itself is wedged.
HEALTH_URL="${HEALTH_URL:-http://localhost:8080/actuator/health/liveness}"
MAX_FAILURES="${MAX_FAILURES:-3}"
CURL_TIMEOUT_SECONDS="${CURL_TIMEOUT_SECONDS:-10}"

PID_FILE="${PROJECT_DIR}/bridge.pid"
STATE_FILE="${PROJECT_DIR}/.keepalive-failures"
APP_LOG="${PROJECT_DIR}/logs/bridge-console.log"
HEAPDUMP_DIR="${PROJECT_DIR}/heapdumps"
LOCK_FILE="${PROJECT_DIR}/.keepalive.lock"

# Serialize runs: a manual invocation racing the cyclic job (or an overlapping
# cycle stuck in the kill-wait loop) must not double-start or fight over the
# pid file / failure counter. Non-blocking: the loser exits clean.
exec 9>"$LOCK_FILE"
if ! flock -n 9; then
    echo "Another keepalive run is active; exiting"
    exit 0
fi

JAR_PATH=$(ls "${PROJECT_DIR}"/target/mq-kafka-bridge-*.jar 2>/dev/null | head -1)
if [ -z "${JAR_PATH}" ]; then
    echo "KEEPALIVE: START-FAIL - no jar under ${PROJECT_DIR}/target"
    exit 1
fi

is_bridge_jvm() {
    # True if the pid is the SUPERVISED bridge: a java process running OUR jar,
    # consuming (listener-enabled=true), and not one of the diagnostic modes.
    #
    # A plain substring match on "mq-kafka-bridge" is not enough: the checkout
    # directory name contains it too, so a recycled PID running e.g.
    # `tail -f .../logs/bridge-console.log` would pass.
    #
    # The diagnostic exclusions are what make this correct in practice. monitor,
    # validate-only, component-test and replay all run the SAME jar from the SAME
    # directory, so a jar-only match counts them as bridges: leftover monitor JVMs
    # made this script report "multiple bridge processes running" every cycle and
    # refuse to supervise anything, leaving the real bridge unwatched for weeks.
    # /proc cmdline is NUL-separated, hence the tr.
    local cmdline
    cmdline=$(tr '\0' ' ' < "/proc/${1}/cmdline" 2>/dev/null) || return 1

    case "$cmdline" in
        *"java "*"${PROJECT_DIR}/target/mq-kafka-bridge-"*.jar*) ;;
        *) return 1 ;;
    esac
    case "$cmdline" in
        *--bridge.monitor.enabled=true*|*--bridge.component-test=*|\
        *--bridge.validate-only=true*|*--bridge.replay=*) return 1 ;;
    esac
    # An instance started WITHOUT the listener is not the 24/7 consumer. It is not
    # adopted on purpose: it still holds the health port, so the caller's
    # foreign-process guard reports it and asks for a human instead of either
    # killing it or silently supervising something that consumes nothing.
    case "$cmdline" in
        *--bridge.mq.listener-enabled=true*) return 0 ;;
    esac
    return 1
}

bridge_procs() {
    # Supervised bridges only — same predicate as is_bridge_jvm, applied to every
    # JVM running this deployment's jar. Anchored to PROJECT_DIR so we never adopt
    # a bridge from another checkout, or a non-java process (rsync, sha256sum, an
    # editor) that merely mentions the jar name.
    local pid
    for pid in $(pgrep -f "java .*${PROJECT_DIR}/target/mq-kafka-bridge-[^ ]*\.jar" || true); do
        is_bridge_jvm "$pid" && echo "$pid"
    done
}

bridge_pid() {
    # PID from file, verified alive and actually our jar (guards stale files
    # whose PID was recycled by an unrelated process)
    local pid
    pid=$(cat "$PID_FILE" 2>/dev/null) || return 1
    [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null || return 1
    is_bridge_jvm "$pid" || return 1
    echo "$pid"
}

check_health() {
    # Prints the HTTP status, or 000 when nothing answers. NOTE: on failure
    # curl still prints 000 itself, so REPLACE the output rather than appending
    # (an `|| echo 000` inside the substitution yields "000000").
    local code
    code=$(curl -s -o /dev/null -m "$CURL_TIMEOUT_SECONDS" -w '%{http_code}' "$HEALTH_URL") || code="000"
    echo "$code"
}

start_bridge() {
    mkdir -p "$(dirname "$APP_LOG")" "$HEAPDUMP_DIR"

    # Same JAAS handling as run-test-env.sh: pass the file as a JVM flag so it
    # applies before anything in the app initializes Kerberos/JAAS state.
    local jaas_opt=""
    if [ -n "${KAFKA_JAAS_CONFIG_PATH:-}" ]; then
        jaas_opt="-Djava.security.auth.login.config=${KAFKA_JAAS_CONFIG_PATH}"
        echo "Using JAAS config: ${KAFKA_JAAS_CONFIG_PATH}"
        if [ ! -r "${KAFKA_JAAS_CONFIG_PATH}" ]; then
            echo "WARNING: JAAS file is missing or unreadable: ${KAFKA_JAAS_CONFIG_PATH}"
        fi
    elif [ -z "${KAFKA_SASL_JAAS_CONFIG:-}" ]; then
        echo "WARNING: neither KAFKA_JAAS_CONFIG_PATH nor KAFKA_SASL_JAAS_CONFIG is set;"
        echo "         Kafka SASL/GSSAPI has no login config and publishing/health will fail."
    fi

    # Cap console-log growth: keep exactly one previous generation. This stream
    # duplicates the rolling application log (which owns long-term retention);
    # this file only needs enough history to diagnose the LAST failed start.
    [ -f "$APP_LOG" ] && mv -f "$APP_LOG" "${APP_LOG}.1"

    # setsid + nohup + full fd redirection so the JVM survives this script
    # (and the Control-M agent's process cleanup) exiting
    setsid nohup java \
        ${jaas_opt} \
        -XX:+ExitOnOutOfMemoryError \
        -XX:+HeapDumpOnOutOfMemoryError \
        -XX:HeapDumpPath="$HEAPDUMP_DIR" \
        -jar "$JAR_PATH" \
        --spring.profiles.active="$PROFILE" \
        --bridge.mq.listener-enabled=true \
        >> "$APP_LOG" 2>&1 < /dev/null &
    local new_pid=$!
    rm -f "$STATE_FILE"
    sleep 5
    if ! kill -0 "$new_pid" 2>/dev/null || ! is_bridge_jvm "$new_pid"; then
        # $! is setsid's pid; when setsid forks instead of exec-ing (manual
        # runs from a job-control shell) the JVM lives on under another pid —
        # find it before declaring failure.
        new_pid=$(bridge_procs | head -1)
    fi
    if [ -n "$new_pid" ] && kill -0 "$new_pid" 2>/dev/null; then
        # Written only on confirmed success so the pid file is never a lie
        echo "$new_pid" > "$PID_FILE"
        echo "KEEPALIVE: STARTED - pid ${new_pid}, profile ${PROFILE}, log ${APP_LOG}"
        exit 0
    else
        echo "KEEPALIVE: START-FAIL - process exited immediately, check ${APP_LOG}"
        exit 1
    fi
}

if ! pid=$(bridge_pid); then
    # Pid file missing/stale (manual start, deleted file, or overwritten by a
    # previous failed duplicate-start). Before launching — which would only die
    # on a port conflict — look for a bridge that is already running and adopt
    # it by re-pointing the pid file at it.
    mapfile -t running < <(bridge_procs)
    if [ "${#running[@]}" -gt 1 ]; then
        echo "KEEPALIVE: START-FAIL - multiple bridge processes running (pids: ${running[*]});"
        echo "  refusing to guess which to supervise - kill the extras, then rerun"
        exit 1
    elif [ "${#running[@]}" -eq 1 ]; then
        pid="${running[0]}"
        echo "$pid" > "$PID_FILE"
        # An adopted pid is a different process from the one that accumulated
        # the failure streak — start its health record from zero, or a bridge
        # still mid-startup inherits 2/3 and gets killed one cycle later.
        rm -f "$STATE_FILE"
        echo "KEEPALIVE: ADOPTED - running bridge found (pid ${pid}), pid file re-pointed"
    else
        # No bridge process — but if something ELSE answers on the health port,
        # a start is doomed (bind failure) and would mask the real problem.
        http_code=$(check_health)
        if [ "$http_code" != "000" ]; then
            echo "KEEPALIVE: START-FAIL - no mq-kafka-bridge process, but ${HEALTH_URL} answered HTTP ${http_code};"
            echo "  a foreign process holds the port - refusing to start a duplicate. Check: ss -tlnp | grep 8080"
            exit 1
        fi
        echo "Bridge process not running; starting it"
        start_bridge
    fi
fi

http_code=$(check_health)

if [ "$http_code" = "200" ]; then
    rm -f "$STATE_FILE"
    echo "Bridge healthy (pid ${pid})"
    exit 0
fi

failures=$(cat "$STATE_FILE" 2>/dev/null || echo 0)
failures=$((failures + 1))

if [ "$failures" -ge "$MAX_FAILURES" ]; then
    echo "Health check failed ${failures}x consecutively (last HTTP ${http_code}); restarting pid ${pid}"
    kill "$pid" 2>/dev/null
    for _ in $(seq 1 12); do kill -0 "$pid" 2>/dev/null || break; sleep 5; done
    if kill -0 "$pid" 2>/dev/null; then
        kill -9 "$pid" 2>/dev/null
        sleep 2
    fi
    if kill -0 "$pid" 2>/dev/null; then
        # Unkillable (likely D-state on a hung HDFS/NFS mount) — starting now
        # would just die on the port bind and reset the failure counter,
        # looping forever. Page instead.
        echo "KEEPALIVE: START-FAIL - pid ${pid} survived kill -9; not starting a duplicate"
        exit 1
    fi
    start_bridge
else
    echo "Health check failure ${failures}/${MAX_FAILURES} (HTTP ${http_code}); not restarting yet"
    echo "$failures" > "$STATE_FILE"
    exit 0
fi
