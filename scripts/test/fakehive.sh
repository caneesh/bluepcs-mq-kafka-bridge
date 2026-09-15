#!/bin/bash
# Hive -> SQLite adapter used by audit-script-scenarios.py. Env: FAKEHIVE_DB (fixture db
# attached as the "bluepcs" schema), FAKEHIVE_MAIN (scratch db). Rewrites the handful of
# Hive-only functions the scripts use; everything else is standard SQL.
# Hive -> SQLite adapter for exercising the audit scripts locally.
Q="$1"
Q="${Q//get_json_object(/json_extract(}"
Q="$(echo "$Q" | sed -E "s/date_sub\(current_date, *([0-9]+)\)/date('now','-\1 days')/g; s/collect_set\(event_type\)/group_concat(DISTINCT event_type)/g; s/INSERT INTO TABLE ([A-Za-z_.]+) PARTITION \(run_dt='[^']*'\) VALUES/INSERT INTO \1 VALUES/")"
sqlite3 -separator $'\t' "$FAKEHIVE_MAIN" "ATTACH '$FAKEHIVE_DB' AS bluepcs; $Q"
