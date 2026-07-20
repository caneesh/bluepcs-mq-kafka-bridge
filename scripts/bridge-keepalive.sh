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
#   - bridge process dead                     -> start it (detached), exit 0
#   - bridge alive but unhealthy N runs in a
#     row (default 3)                         -> kill it, start fresh, exit 0
#   - start attempted but process not up      -> exit 1 (turn this red in
#                                                Control-M and alert)
#
# Output markers for Control-M On-Do text matching:
#   "KEEPALIVE: STARTED"    - bridge was (re)started this run
#   "KEEPALIVE: START-FAIL" - start attempt failed
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

PROFILE="${1:-test-env}"
HEALTH_URL="${HEALTH_URL:-http://localhost:8080/actuator/health}"
MAX_FAILURES="${MAX_FAILURES:-3}"
CURL_TIMEOUT_SECONDS="${CURL_TIMEOUT_SECONDS:-10}"

PID_FILE="${PROJECT_DIR}/bridge.pid"
STATE_FILE="${PROJECT_DIR}/.keepalive-failures"
APP_LOG="${PROJECT_DIR}/logs/bridge-console.log"
HEAPDUMP_DIR="${PROJECT_DIR}/heapdumps"

# Load .env if present (same convention as the other scripts)
if [ -f "${PROJECT_DIR}/.env" ]; then
    set -a
    # shellcheck disable=SC1091
    source "${PROJECT_DIR}/.env"
    set +a
fi

JAR_PATH=$(ls "${PROJECT_DIR}"/target/mq-kafka-bridge-*.jar 2>/dev/null | head -1)
if [ -z "${JAR_PATH}" ]; then
    echo "KEEPALIVE: START-FAIL - no jar under ${PROJECT_DIR}/target"
    exit 1
fi

bridge_pid() {
    # PID from file, verified alive and actually our jar (guards stale files
    # whose PID was recycled by an unrelated process)
    local pid
    pid=$(cat "$PID_FILE" 2>/dev/null) || return 1
    [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null || return 1
    grep -qF "mq-kafka-bridge" "/proc/${pid}/cmdline" 2>/dev/null || return 1
    echo "$pid"
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
    echo $! > "$PID_FILE"
    rm -f "$STATE_FILE"
    sleep 5
    if bridge_pid > /dev/null; then
        echo "KEEPALIVE: STARTED - pid $(cat "$PID_FILE"), profile ${PROFILE}, log ${APP_LOG}"
        exit 0
    else
        echo "KEEPALIVE: START-FAIL - process exited immediately, check ${APP_LOG}"
        exit 1
    fi
}

pid=$(bridge_pid) || { echo "Bridge process not running; starting it"; start_bridge; }

http_code=$(curl -s -o /dev/null -m "$CURL_TIMEOUT_SECONDS" -w "%{http_code}" "$HEALTH_URL" || echo "000")

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
    kill -0 "$pid" 2>/dev/null && kill -9 "$pid" 2>/dev/null
    start_bridge
else
    echo "Health check failure ${failures}/${MAX_FAILURES} (HTTP ${http_code}); not restarting yet"
    echo "$failures" > "$STATE_FILE"
    exit 0
fi
