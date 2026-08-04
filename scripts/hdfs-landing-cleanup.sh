#!/bin/bash
# =============================================================================
# HDFS Landing Directory Retention Sweep
# =============================================================================
# SAFETY-NET sweep behind the normal file lifecycle (see DEPLOYMENT_CHECKLIST.md
# "HDFS File Lifecycle"). The CONSUMER owns the normal path: it moves each
# processed file out of the landing directory itself. This sweep only catches
# what that leaves behind:
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
# NOTE: a file still in landing after LANDING_RETENTION_DAYS was NEVER
# processed by the consumer (the consumer moves processed files out itself).
# Archiving it keeps the landing dir finite, but afterwards a redelivery of
# its message is skipped as an already-processed duplicate by the consumer's
# BridgeMessageResolver — so investigate the monitor's exit-3 backlog alerts
# (which fire within ~30 min of a consumer stall) long before files age out
# here. This sweep is garbage collection, not a replay window.
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
# A FAILED listing (expired ticket, namenode down, ACL loss) must abort the run
# with the real error — silently treating it as an empty directory would print
# "scanned 0, archived 0 ... Done" forever while retention quietly stops. The
# function runs in a process-substitution subshell, so it signals failure via a
# marker file that the parent checks after each pass.
LS_ERR_FILE=$(mktemp)
trap 'rm -f "$LS_ERR_FILE" "${LS_ERR_FILE}.failed"' EXIT

list_files_with_epoch() {
    local listing
    if ! listing=$(hdfs dfs -ls "$1" 2>"$LS_ERR_FILE"); then
        touch "${LS_ERR_FILE}.failed"
        return 0
    fi
    printf '%s\n' "$listing" | awk '$1 ~ /^-/ { print $6" "$7"\t"$8 }' \
    | while IFS=$'\t' read -r datetime path; do
        epoch=$(date -d "$datetime" +%s 2>/dev/null) || continue
        printf '%s\t%s\n' "$epoch" "$path"
    done
}

abort_if_listing_failed() {
    if [ -f "${LS_ERR_FILE}.failed" ]; then
        echo "ERROR: HDFS listing of $1 failed — aborting (nothing was silently skipped):" >&2
        cat "$LS_ERR_FILE" >&2
        exit 2
    fi
}

# Per-file failures (e.g. an archive name collision) are counted and reported at
# the end instead of aborting the whole sweep mid-pass under set -e.
op_failures=0
move_or_count() {
    run_or_echo hdfs dfs -mv "$1" "$2" || {
        echo "WARN: failed to archive: $1" >&2
        op_failures=$((op_failures + 1))
        return 0
    }
}
remove_or_count() {
    run_or_echo hdfs dfs -rm "$1" || {
        echo "WARN: failed to delete: $1" >&2
        op_failures=$((op_failures + 1))
        return 0
    }
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
        move_or_count "$path" "$ARCHIVE/"
        archived=$((archived + 1))
    fi
done < <(list_files_with_epoch "$LANDING")
abort_if_listing_failed "$LANDING"
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
        remove_or_count "$path"
        tmp_deleted=$((tmp_deleted + 1))
    fi
done < <(list_files_with_epoch "$LANDING")
abort_if_listing_failed "$LANDING"
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
        remove_or_count "$path"
        purged=$((purged + 1))
    fi
done < <(list_files_with_epoch "$ARCHIVE")
abort_if_listing_failed "$ARCHIVE"
echo "Archive pass: deleted ${purged} file(s)"

echo ""
echo "Done: archived=${archived}, tmp_deleted=${tmp_deleted}, purged=${purged}, op_failures=${op_failures}, dry_run=${DRY_RUN}"
if [ "$op_failures" -gt 0 ]; then
    echo "WARNING: ${op_failures} file operation(s) failed — see WARN lines above"
    exit 1
fi
