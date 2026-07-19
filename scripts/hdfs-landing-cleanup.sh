#!/bin/bash
# =============================================================================
# HDFS Landing Directory Retention Sweep
# =============================================================================
# Implements the file lifecycle for bridge payload files
# (see DEPLOYMENT_CHECKLIST.md "HDFS File Lifecycle"):
#
#   landing (<HDFS_BASE_PATH>)            files older than LANDING_RETENTION_DAYS
#     └→ archive (<HDFS_ARCHIVE_PATH>)    files older than ARCHIVE_RETENTION_DAYS
#          └→ deleted
#
# Also deletes orphaned *.tmp files (crashed safe-writes) older than 1 day
# from the landing directory.
#
# Scope guarantees:
#   - Only FILES directly in the landing dir are touched (no recursion) —
#     the errors/ and archive/ subdirectories are never swept by the landing
#     pass. The errors/ quarantine dir is intentionally NOT auto-cleaned:
#     quarantined payloads are the only copy of an unparseable message and
#     must be reviewed/deleted manually.
#   - Only *.json and *.json.tmp names are considered.
#
# Usage:
#   ./hdfs-landing-cleanup.sh [--dry-run]
#
# Configuration (sourced from project .env if present, else environment):
#   HDFS_BASE_PATH            landing dir   (required)
#   HDFS_ARCHIVE_PATH         archive dir   (default: <HDFS_BASE_PATH>/archive)
#   LANDING_RETENTION_DAYS    move to archive after N days   (default: 7)
#   ARCHIVE_RETENTION_DAYS    delete from archive after N days (default: 30)
#   HDFS_KERBEROS_PRINCIPAL / HDFS_KERBEROS_KEYTAB
#                             if both set, kinit is run first (cron-safe)
#
# Cron example (daily at 02:15):
#   15 2 * * * /path/to/scripts/hdfs-landing-cleanup.sh \
#       >> /var/log/bluepcs/hdfs-landing-cleanup.log 2>&1
#
# NOTE: LANDING_RETENTION_DAYS is the consumer's replay window — a Kafka
# message re-read after the file was archived is skipped as a duplicate by
# the consumer's BridgeMessageResolver. Size it to comfortably exceed the
# longest expected consumer outage.
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

DRY_RUN=false
if [ "${1:-}" = "--dry-run" ]; then
    DRY_RUN=true
fi

# Load .env if present (same convention as the run scripts)
if [ -f "${PROJECT_DIR}/.env" ]; then
    set -a
    # shellcheck disable=SC1091
    source "${PROJECT_DIR}/.env"
    set +a
fi

LANDING="${HDFS_BASE_PATH:-}"
if [ -z "$LANDING" ]; then
    echo "ERROR: HDFS_BASE_PATH is not set (in .env or environment)"
    exit 2
fi
LANDING="${LANDING%/}"
ARCHIVE="${HDFS_ARCHIVE_PATH:-${LANDING}/archive}"
ARCHIVE="${ARCHIVE%/}"
LANDING_DAYS="${LANDING_RETENTION_DAYS:-7}"
ARCHIVE_DAYS="${ARCHIVE_RETENTION_DAYS:-30}"
TMP_DAYS=1

echo "============================================"
echo "HDFS Landing Cleanup - $(date '+%Y-%m-%d %H:%M:%S')"
echo "============================================"
echo "Landing dir:        ${LANDING}"
echo "Archive dir:        ${ARCHIVE}"
echo "Landing retention:  ${LANDING_DAYS} days (then archive)"
echo "Archive retention:  ${ARCHIVE_DAYS} days (then delete)"
echo "Dry run:            ${DRY_RUN}"
echo ""

# Kerberos login when configured (needed for cron, where no ticket cache exists)
if [ -n "${HDFS_KERBEROS_PRINCIPAL:-}" ] && [ -n "${HDFS_KERBEROS_KEYTAB:-}" ]; then
    echo "kinit as ${HDFS_KERBEROS_PRINCIPAL}"
    kinit -kt "${HDFS_KERBEROS_KEYTAB}" "${HDFS_KERBEROS_PRINCIPAL}"
fi

NOW_EPOCH=$(date +%s)
LANDING_CUTOFF=$((NOW_EPOCH - LANDING_DAYS * 86400))
ARCHIVE_CUTOFF=$((NOW_EPOCH - ARCHIVE_DAYS * 86400))
TMP_CUTOFF=$((NOW_EPOCH - TMP_DAYS * 86400))

run_or_echo() {
    if [ "$DRY_RUN" = true ]; then
        echo "DRY-RUN: $*"
    else
        "$@"
    fi
}

# Emits "epoch<TAB>path" for plain files (no directories) directly in $1.
# hdfs dfs -ls columns: perms repl owner group size date(yyyy-MM-dd) time(HH:mm) path
list_files_with_epoch() {
    hdfs dfs -ls "$1" 2>/dev/null | awk '$1 ~ /^-/ { print $6" "$7"\t"$8 }' \
    | while IFS=$'\t' read -r datetime path; do
        epoch=$(date -d "$datetime" +%s 2>/dev/null) || continue
        printf '%s\t%s\n' "$epoch" "$path"
    done
}

run_or_echo hdfs dfs -mkdir -p "$ARCHIVE"

# ---------------------------------------------------------------------------
# Pass 1: landing *.json older than LANDING_DAYS -> archive
# ---------------------------------------------------------------------------
archived=0
scanned=0
while IFS=$'\t' read -r epoch path; do
    case "$path" in
        "$LANDING"/*.json) ;;
        *) continue ;;
    esac
    scanned=$((scanned + 1))
    if [ "$epoch" -lt "$LANDING_CUTOFF" ]; then
        run_or_echo hdfs dfs -mv "$path" "$ARCHIVE/"
        archived=$((archived + 1))
    fi
done < <(list_files_with_epoch "$LANDING")
echo "Landing pass: scanned ${scanned} json file(s), archived ${archived}"

# ---------------------------------------------------------------------------
# Pass 2: orphaned landing *.json.tmp older than TMP_DAYS -> delete
# (leftovers of crashed safe-writes; the bridge rewrites idempotently)
# ---------------------------------------------------------------------------
tmp_deleted=0
while IFS=$'\t' read -r epoch path; do
    case "$path" in
        "$LANDING"/*.json.tmp) ;;
        *) continue ;;
    esac
    if [ "$epoch" -lt "$TMP_CUTOFF" ]; then
        run_or_echo hdfs dfs -rm "$path"
        tmp_deleted=$((tmp_deleted + 1))
    fi
done < <(list_files_with_epoch "$LANDING")
echo "Tmp pass: deleted ${tmp_deleted} orphaned .tmp file(s)"

# ---------------------------------------------------------------------------
# Pass 3: archive *.json older than ARCHIVE_DAYS -> delete
# (plain -rm: respects HDFS trash if the cluster has it enabled)
# ---------------------------------------------------------------------------
purged=0
while IFS=$'\t' read -r epoch path; do
    case "$path" in
        "$ARCHIVE"/*.json) ;;
        *) continue ;;
    esac
    if [ "$epoch" -lt "$ARCHIVE_CUTOFF" ]; then
        run_or_echo hdfs dfs -rm "$path"
        purged=$((purged + 1))
    fi
done < <(list_files_with_epoch "$ARCHIVE")
echo "Archive pass: deleted ${purged} file(s)"

echo ""
echo "Done: archived=${archived}, tmp_deleted=${tmp_deleted}, purged=${purged}, dry_run=${DRY_RUN}"
