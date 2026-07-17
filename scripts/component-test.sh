#!/bin/bash
# =============================================================================
# Component Test Script
# =============================================================================
# Exercises exactly ONE part of the bridge against real infrastructure and
# exits pass/fail. Intended for office-network bring-up testing.
#
# Usage: ./component-test.sh <mq|api|kafka|hdfs> [args] [profile]
#   profile defaults to test-env
#
# Examples:
#   ./component-test.sh hdfs
#   ./component-test.sh api SPSH44PPOIMTO,2026-01-01
#   ./component-test.sh kafka my-scratch-topic
#   ./component-test.sh mq "" prod
#
# Exit codes (propagated from the JVM):
#   0 - PASS
#   1 - FAIL
#   2 - Bad mode/args
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

MODE="$1"
ARGS="$2"
PROFILE="${3:-test-env}"

if [ -z "$MODE" ]; then
    echo "Usage: ./component-test.sh <mq|api|kafka|hdfs> [args] [profile]"
    exit 2
fi

case "$MODE" in
    mq|api|kafka|hdfs) ;;
    *)
        echo "ERROR: unknown mode '$MODE'. Expected one of: mq, api, kafka, hdfs"
        echo "Usage: ./component-test.sh <mq|api|kafka|hdfs> [args] [profile]"
        exit 2
        ;;
esac

# Load .env if present. Note: .env values override anything already exported
# in the shell, including blank assignments — remove a line from .env to use
# an exported value instead.
if [ -f "${PROJECT_DIR}/.env" ]; then
    echo "Loading environment from ${PROJECT_DIR}/.env"
    set -a
    # shellcheck disable=SC1091
    source "${PROJECT_DIR}/.env"
    set +a
fi

# BOTH secrets are required for EVERY mode, not just the mode being tested.
# The test-env profile declares kafka.truststore-password and security.client-secret
# with NO defaults, so Spring fails placeholder resolution at startup regardless of
# which component we intend to exercise. Require both here for a clear message.
MISSING_VARS=0
for var in KAFKA_TRUSTSTORE_PASSWORD OAUTH_CLIENT_ID OAUTH_CLIENT_SECRET API_PASSWORD; do
    if [ -z "${!var}" ]; then
        echo "MISSING: $var"
        MISSING_VARS=$((MISSING_VARS + 1))
    fi
done

if [ $MISSING_VARS -gt 0 ]; then
    echo ""
    echo "ERROR: $MISSING_VARS required secret(s) not set."
    echo "Copy .env.template to .env in the project root and fill in the values,"
    echo "or export the variables before running this script."
    exit 3
fi

echo "============================================"
echo "MQ-Kafka Bridge - Component Test"
echo "============================================"
echo "Mode: ${MODE}"
echo "Args: ${ARGS}"
echo "Profile: ${PROFILE}"
echo ""

# Check if JAR exists, if not build
JAR_FILE="${PROJECT_DIR}/target/mq-kafka-bridge-*.jar"
if ! ls ${JAR_FILE} 1> /dev/null 2>&1; then
    echo "JAR not found. Building..."
    cd "$PROJECT_DIR"
    mvn package -DskipTests -q
    echo "Build complete."
    echo ""
fi

JAR_PATH=$(ls ${JAR_FILE} | head -1)
echo "JAR: ${JAR_PATH}"
echo ""

echo "Starting component test..."
echo "============================================"
echo ""

set +e
# Set the JAAS file as a JVM flag when configured: this applies before the JVM
# starts anything, so it cannot be defeated by JAAS-configuration caching or
# bean initialization order inside the app.
JAAS_OPT=""
if [ -n "${KAFKA_JAAS_CONFIG_PATH}" ]; then
    JAAS_OPT="-Djava.security.auth.login.config=${KAFKA_JAAS_CONFIG_PATH}"
    echo "Using JAAS config: ${KAFKA_JAAS_CONFIG_PATH}"
fi

java ${JAAS_OPT} -jar "${JAR_PATH}" \
    --spring.profiles.active="$PROFILE" \
    --bridge.component-test="$MODE" \
    --bridge.component-test-args="$ARGS" \
    --bridge.mq.listener-enabled=false \
    --bridge.validate-only=false

EXIT_CODE=$?
set -e

echo ""
echo "============================================"
case $EXIT_CODE in
    0) echo "RESULT: PASS" ;;
    1) echo "RESULT: FAIL" ;;
    2) echo "RESULT: FAIL (bad mode/args)" ;;
    *) echo "RESULT: FAIL (exit code: $EXIT_CODE)" ;;
esac
echo "============================================"

exit $EXIT_CODE
