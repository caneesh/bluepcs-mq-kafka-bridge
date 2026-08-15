#!/bin/bash
# =============================================================================
# Verify a generate-test-messages.sh batch against its identifier list
# =============================================================================
# Written for batches produced BEFORE the whitespace fix: the old generator
# deleted whitespace everywhere, so a source line "SPSH 44PPO" was silently
# rewritten to the different-but-valid identifier "SPSH44PPO" and a file was
# written for it with no warning. Driving a catch-up batch, that means the wrong
# plan was enriched and the intended one was never generated.
#
# Reports three things:
#   MANGLED  - a file exists under a DIFFERENT identifier than the source line
#              (the old whitespace bug). Regenerate these with the fixed script.
#   MISSING  - a valid source identifier with no file (duplicate line, or the
#              run stopped early)
#   ORPHAN   - a file that cannot be traced back to any line in the list
#
# Usage:
#   ./check-generated-messages.sh <identifier-list-file> [generated-dir]
#       generated-dir defaults to target/test-messages
#
# Exit codes:
#   0 - batch matches the list (no mangled identifiers)
#   1 - at least one mangled identifier found
#   2 - bad arguments / unreadable list / missing directory
# =============================================================================

set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

LIST_FILE="${1:-}"
OUT_DIR="${2:-${PROJECT_DIR}/target/test-messages}"

if [ -z "$LIST_FILE" ]; then
    echo "Usage: ./check-generated-messages.sh <identifier-list-file> [generated-dir]" >&2
    exit 2
fi
if [ ! -r "$LIST_FILE" ]; then
    echo "ERROR: cannot read identifier list: ${LIST_FILE}" >&2
    exit 2
fi
if [ ! -d "$OUT_DIR" ]; then
    echo "ERROR: generated directory not found: ${OUT_DIR}" >&2
    echo "       pass it explicitly: ./check-generated-messages.sh <list> <dir>" >&2
    exit 2
fi

FILE_LIST="$(mktemp)"
trap 'rm -f "$FILE_LIST"' EXIT
# shellcheck disable=SC2010
ls -1 "$OUT_DIR" 2>/dev/null | sed -n 's/\.json$//p' | LC_ALL=C sort > "$FILE_LIST"

echo "============================================"
echo "List:      ${LIST_FILE}"
echo "Generated: ${OUT_DIR}"
echo "============================================"

# Pass 1 loads the filenames on disk; pass 2 replays the OLD normalisation over
# each source line and compares. Charset must match the generator's exactly.
awk '
    NR == FNR { have[$0] = 1; files++; next }
    {
        line = $0
        sub(/\r$/, "", line)                        # tolerate CRLF
        sub(/,.*$/, "", line)                       # first CSV field
        gsub(/^[ \t]+|[ \t]+$/, "", line)           # trim only the ends
        if (line == "" || line ~ /^#/) next
        sources++
        seen_src[line]++

        collapsed = line
        gsub(/[ \t]+/, "", collapsed)               # what the OLD script produced

        if (collapsed != line) {
            if (collapsed ~ /^[A-Za-z0-9._-]+$/ && (collapsed in have)) {
                printf "MANGLED  line %d: source \"%s\" -> wrote %s.json\n", FNR, line, collapsed
                mangled++
                accounted[collapsed] = 1
            } else {
                printf "skipped  line %d: \"%s\" (contains whitespace; no file written)\n", FNR, line
            }
            next
        }
        if (line !~ /^[A-Za-z0-9._-]+$/) {
            printf "skipped  line %d: \"%s\" (unsafe characters; no file written)\n", FNR, line
            next
        }
        if (line in have) {
            accounted[line] = 1
        } else if (seen_src[line] == 1) {
            printf "MISSING  line %d: %s has no file\n", FNR, line
            missing++
        }
    }
    END {
        for (f in have) if (!(f in accounted)) { printf "ORPHAN   %s.json not traceable to the list\n", f; orphans++ }
        printf "\n%d source identifier(s), %d file(s) on disk\n", sources, files
        printf "mangled=%d  missing=%d  orphan=%d\n", mangled + 0, missing + 0, orphans + 0
        if (mangled > 0) {
            print "\nRegenerate the MANGLED entries with the fixed generator (git pull first)."
            print "If that batch was already published, the collapsed identifier was enriched"
            print "instead of the intended one - the intended plan still needs catching up."
            exit 1
        }
        print "\nNo mangled identifiers."
    }
' "$FILE_LIST" "$LIST_FILE"
