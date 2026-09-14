#!/bin/bash
# =============================================================================
# PMM Landing Tree Retention Sweep (mq-pmm-bridge)
# =============================================================================
# The PMM bridge lands one XML file per message under a time-partitioned tree:
#
#   <PMM_HDFS_BASE_PATH>/<yyyy-MM-dd>/<HH>/<eventId>.xml      HH = window start hour
#   <PMM_HDFS_BASE_PATH>/errors/<eventId>.xml                 quarantine (never swept)
#
# This sweep works on WHOLE DATE DIRECTORIES, never on individual landing files:
#
#   <base>/<date>            date older than PMM_LANDING_RETENTION_DAYS
#     └→ <archive>/<date>    date older than PMM_ARCHIVE_RETENTION_DAYS
#          └→ deleted
#
# and deletes orphaned *.xml.tmp files (crashed safe-writes) older than 1 day
# anywhere under the date directories.
#
# Do NOT point hdfs-landing-cleanup.sh at this tree: it is flat-directory and
# .json-only by design. Do NOT point this script at the PMM+ landing directory.
#
# Usage:
#   ./pmm-hdfs-cleanup.sh [--dry-run]
#
# Configuration (sourced from the PMM bridge's .env if present, else environment):
#   PMM_HDFS_BASE_PATH          landing tree root        (required)
#   PMM_HDFS_ARCHIVE_PATH       archive root             (default: <base>/archive)
#   PMM_LANDING_RETENTION_DAYS  archive a date after N days   (default: 7)
#   PMM_ARCHIVE_RETENTION_DAYS  delete from archive after M days (default: 30)
#   HDFS_KERBEROS_PRINCIPAL / HDFS_KERBEROS_KEYTAB   kinit first when both set
#
# Cron example (daily at 02:45):
#   45 2 * * * /path/to/scripts/pmm-hdfs-cleanup.sh >> /var/log/bluepcs/pmm-hdfs-cleanup.log 2>&1
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

DRY_RUN=false
if [ "${1:-}" = "--dry-run" ]; then
    DRY_RUN=true
fi

if [ -f "${PROJECT_DIR}/.env" ]; then
    set -a
    # shellcheck disable=SC1091
    source "${PROJECT_DIR}/.env"
    set +a
fi

BASE="${PMM_HDFS_BASE_PATH:-}"
if [ -z "$BASE" ]; then
    echo "ERROR: PMM_HDFS_BASE_PATH is not set (in .env or environment)"
    exit 2
fi
BASE="${BASE%/}"
ARCHIVE="${PMM_HDFS_ARCHIVE_PATH:-${BASE}/archive}"
ARCHIVE="${ARCHIVE%/}"
LANDING_DAYS="${PMM_LANDING_RETENTION_DAYS:-7}"
ARCHIVE_DAYS="${PMM_ARCHIVE_RETENTION_DAYS:-30}"
TMP_DAYS=1

echo "============================================"
echo "PMM HDFS Cleanup - $(date '+%Y-%m-%d %H:%M:%S')"
echo "============================================"
echo "Landing tree:       ${BASE}"
echo "Archive tree:       ${ARCHIVE}"
echo "Landing retention:  ${LANDING_DAYS} days (then archive the date directory)"
echo "Archive retention:  ${ARCHIVE_DAYS} days (then delete the date directory)"
echo "Dry run:            ${DRY_RUN}"
echo ""

if [ -n "${HDFS_KERBEROS_PRINCIPAL:-}" ] && [ -n "${HDFS_KERBEROS_KEYTAB:-}" ]; then
    echo "kinit as ${HDFS_KERBEROS_PRINCIPAL}"
    kinit -kt "${HDFS_KERBEROS_KEYTAB}" "${HDFS_KERBEROS_PRINCIPAL}"
fi

TODAY_EPOCH=$(date -u -d "$(date -u '+%Y-%m-%d')" +%s)
LANDING_CUTOFF=$((TODAY_EPOCH - LANDING_DAYS * 86400))
ARCHIVE_CUTOFF=$((TODAY_EPOCH - ARCHIVE_DAYS * 86400))
NOW_EPOCH=$(date +%s)
TMP_CUTOFF=$((NOW_EPOCH - TMP_DAYS * 86400))

run_or_echo() {
    if [ "$DRY_RUN" = true ]; then
        echo "DRY-RUN: $*"
    else
        "$@"
    fi
}

# Date directories directly under $1 whose name parses as yyyy-MM-dd, emitted as
# "epoch<TAB>path". A FAILED listing (expired ticket, namenode down, ACL loss) must
# abort the run with the real error: silently treating it as "no date directories"
# would print "0 archived, 0 deleted ... Done" forever while retention quietly
# stops. The function runs in a process-substitution subshell, where `exit` cannot
# reach the parent, so it signals failure through a marker file that the parent
# checks after each pass (same pattern as hdfs-landing-cleanup.sh).
LS_ERR_FILE=$(mktemp)
trap 'rm -f "$LS_ERR_FILE" "${LS_ERR_FILE}.failed"' EXIT

list_date_dirs() {
    local listing
    if ! listing=$(hdfs dfs -ls "$1" 2>"$LS_ERR_FILE"); then
        if hdfs dfs -test -d "$1" 2>/dev/null; then
            touch "${LS_ERR_FILE}.failed"
        fi
        # A missing directory (no traffic yet) is legitimately empty
        return 0
    fi
    printf '%s\n' "$listing" | awk '$1 ~ /^d/ {print $NF}' | while read -r path; do
        name="${path##*/}"
        if [[ "$name" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] && date -u -d "$name" +%s >/dev/null 2>&1; then
            printf '%s\t%s\n' "$(date -u -d "$name" +%s)" "$path"
        fi
    done
}

abort_if_listing_failed() {
    if [ -f "${LS_ERR_FILE}.failed" ]; then
        echo "ERROR: HDFS listing of $1 failed — aborting (nothing was silently skipped):" >&2
        cat "$LS_ERR_FILE" >&2
        exit 3
    fi
}

# Per-directory failures (ACL, archive name collision) are counted and reported at
# the end instead of aborting mid-pass under set -e — or, worse, being swallowed:
# under set -e only the LAST command of `a && b` is checked, so a bare
# `run_or_echo hdfs dfs -mv ... && n=$((n+1))` would hide a failed move.
op_failures=0
move_or_count() {
    run_or_echo hdfs dfs -mv "$1" "$2" || {
        echo "WARN: failed to archive: $1" >&2
        op_failures=$((op_failures + 1))
        return 1
    }
}
remove_or_count() {
    run_or_echo hdfs dfs -rm "$@" || {
        echo "WARN: failed to delete: ${*: -1}" >&2
        op_failures=$((op_failures + 1))
        return 1
    }
}

archived=0; deleted=0; tmp_removed=0

# 1. Landing -> archive: whole date directories older than the landing retention.
#    Today's and the previous days' windows are never touched, so late arrivals
#    (a message whose put time falls in a "closed" window) still land in place.
run_or_echo hdfs dfs -mkdir -p "$ARCHIVE"
while IFS=$'\t' read -r epoch path; do
    [ -z "$path" ] && continue
    if [ "$epoch" -lt "$LANDING_CUTOFF" ]; then
        echo "archive: ${path} -> ${ARCHIVE}/"
        if move_or_count "$path" "${ARCHIVE}/"; then archived=$((archived + 1)); fi
    fi
done < <(list_date_dirs "$BASE")
abort_if_listing_failed "$BASE"

# 2. Archive -> delete: date directories older than the archive retention.
while IFS=$'\t' read -r epoch path; do
    [ -z "$path" ] && continue
    if [ "$epoch" -lt "$ARCHIVE_CUTOFF" ]; then
        echo "delete: ${path}"
        if remove_or_count -r "$path"; then deleted=$((deleted + 1)); fi
    fi
done < <(list_date_dirs "$ARCHIVE")
abort_if_listing_failed "$ARCHIVE"

# 3. Orphaned temp files older than TMP_DAYS under the remaining date directories.
#    Recursive listing is bounded by the retention window (at most LANDING_DAYS
#    date directories x windows per day). errors/ and archive/ are excluded because
#    only yyyy-MM-dd directories are walked. hdfs dfs -ls prints the cluster's
#    local date/time; -u keeps the comparison in the same frame as TMP_CUTOFF only
#    when the cluster runs UTC, so a listing failure here is fatal but a timezone
#    skew of a few hours on a 1-day threshold is tolerated.
while IFS=$'\t' read -r epoch path; do
    [ -z "$path" ] && continue
    if ! recursive=$(hdfs dfs -ls -R "$path" 2>"$LS_ERR_FILE"); then
        echo "ERROR: HDFS recursive listing of $path failed — aborting:" >&2
        cat "$LS_ERR_FILE" >&2
        exit 3
    fi
    while read -r perms _ _ _ _ d t file; do
        case "$perms" in d*) continue ;; esac
        case "$file" in *.xml.tmp) ;; *) continue ;; esac
        mtime=$(date -u -d "$d $t" +%s 2>/dev/null || echo "$NOW_EPOCH")
        if [ "$mtime" -lt "$TMP_CUTOFF" ]; then
            echo "orphan temp: ${file}"
            if remove_or_count "$file"; then tmp_removed=$((tmp_removed + 1)); fi
        fi
    done <<< "$recursive"
done < <(list_date_dirs "$BASE")
abort_if_listing_failed "$BASE"

echo ""
echo "Done: ${archived} date dir(s) archived, ${deleted} deleted, ${tmp_removed} orphan temp file(s) removed, op_failures=${op_failures}, dry_run=${DRY_RUN}"
if [ "$op_failures" -gt 0 ]; then
    echo "WARNING: ${op_failures} HDFS operation(s) failed — see WARN lines above"
    exit 1
fi
