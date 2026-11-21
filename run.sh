#!/usr/bin/env bash
# runs each statement in a sql file one at a time (the cli only takes one per call)
# usage: ./run.sh sql/01_bronze.sql
set -euo pipefail

python3 - "$1" <<'PY' | while IFS= read -r -d '' stmt; do
import sys, re
sql = re.sub(r'--[^\n]*', '', open(sys.argv[1]).read())
for s in sql.split(';'):
    if s.strip():
        sys.stdout.write(s.strip() + '\0')
PY
  echo ">> $(echo "$stmt" | head -1)"
  databricks experimental aitools tools query -p "${DATABRICKS_PROFILE:-DEFAULT}" -o csv "$stmt" > /dev/null
done
echo "done"
