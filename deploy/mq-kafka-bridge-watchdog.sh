#!/bin/bash
# =============================================================================
# Health watchdog for the MQ-Kafka Bridge.
# =============================================================================
# Invoked every minute by mq-kafka-bridge-watchdog.timer. Curls the actuator
# health endpoint and restarts the bridge after N consecutive failures —
# healing states in-app recovery cannot reach (wedged listener container,
# stuck JVM, dead Tomcat).
#
# Design:
# - Only acts while the service is active: an intentionally stopped bridge is
#   never restarted by the watchdog.
# - A single 200 resets the strike counter, so transient blips never restart.
# - State lives in /run (tmpfs) and disappears on reboot, which is correct:
#   a fresh boot starts with a clean slate.
# =============================================================================

set -u

SERVICE="mq-kafka-bridge"
HEALTH_URL="${HEALTH_URL:-http://localhost:8080/actuator/health}"
MAX_FAILURES="${MAX_FAILURES:-3}"
CURL_TIMEOUT_SECONDS="${CURL_TIMEOUT_SECONDS:-10}"
STATE_FILE="/run/${SERVICE}-watchdog.failures"

# Never restart a service that is not supposed to be running
if ! systemctl is-active --quiet "$SERVICE"; then
    rm -f "$STATE_FILE"
    exit 0
fi

http_code=$(curl -s -o /dev/null -m "$CURL_TIMEOUT_SECONDS" -w "%{http_code}" "$HEALTH_URL" || echo "000")

if [ "$http_code" = "200" ]; then
    rm -f "$STATE_FILE"
    exit 0
fi

failures=$(cat "$STATE_FILE" 2>/dev/null || echo 0)
failures=$((failures + 1))

if [ "$failures" -ge "$MAX_FAILURES" ]; then
    echo "Health check failed ${failures}x consecutively (last HTTP ${http_code}); restarting ${SERVICE}"
    rm -f "$STATE_FILE"
    systemctl restart "$SERVICE"
else
    echo "Health check failure ${failures}/${MAX_FAILURES} (HTTP ${http_code}); not restarting yet"
    echo "$failures" > "$STATE_FILE"
fi
