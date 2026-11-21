#!/usr/bin/env bash
# uploads a notebook to the workspace, runs it once on serverless, prints what it returned
# usage: ./run_notebook.sh notebooks/02_silver.py [pip deps...]
set -euo pipefail

PROFILE="${DATABRICKS_PROFILE:-DEFAULT}"
FILE="$1"; shift
NAME=$(basename "$FILE" .py)
USER_NAME=$(databricks current-user me -p "$PROFILE" -o json | python3 -c "import json,sys;print(json.load(sys.stdin)['userName'])")
WS="/Workspace/Users/$USER_NAME/ev-supply-chain-analytics"
DEPS=$(python3 -c 'import json,sys;print(json.dumps(sys.argv[1:]))' "$@")

databricks workspace mkdirs "$WS" -p "$PROFILE"
databricks workspace import "$WS/$NAME" --file "$FILE" --format SOURCE --language PYTHON --overwrite -p "$PROFILE"

RUN=$(databricks jobs submit --no-wait -p "$PROFILE" -o json --json "{
  \"run_name\": \"$NAME\",
  \"tasks\": [{\"task_key\": \"main\", \"notebook_task\": {\"notebook_path\": \"$WS/$NAME\"}, \"environment_key\": \"env\"}],
  \"environments\": [{\"environment_key\": \"env\", \"spec\": {\"client\": \"4\", \"dependencies\": $DEPS}}]
}" | python3 -c "import json,sys;print(json.load(sys.stdin)['run_id'])")
echo "run $RUN started"

while true; do
  read -r STATE RESULT TASK URL < <(databricks jobs get-run "$RUN" -p "$PROFILE" -o json | python3 -c "
import json,sys; d=json.load(sys.stdin); s=d['state']
print(s['life_cycle_state'], s.get('result_state','-'), d['tasks'][0]['run_id'], d['run_page_url'])")
  [[ "$STATE" =~ ^(TERMINATED|SKIPPED|INTERNAL_ERROR)$ ]] && break
  sleep 20
done

echo "$STATE $RESULT"
echo "$URL"
databricks jobs get-run-output "$TASK" -p "$PROFILE" -o json | python3 -c "
import json,sys; d=json.load(sys.stdin)
r=d.get('notebook_output',{}).get('result')
print(json.dumps(json.loads(r),indent=1) if r else d.get('error','') + '\n' + (d.get('error_trace') or '')[-3000:])"
[[ "$RESULT" == "SUCCESS" ]]
