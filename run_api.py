"""runs each statement of a sql file through the sql statement api, keeping whitespace intact
(the cli query tool strips leading spaces per line, which breaks the yaml in metric views)
usage: python3 run_api.py sql/10_metric_views.sql"""
import json, re, subprocess, sys, time

WAREHOUSE = "20729c7c3db8023a"
PROFILE = "DEFAULT"

def api(method, path, body=None):
    cmd = ["databricks", "api", method, path, "-p", PROFILE]
    if body is not None:
        cmd += ["--json", json.dumps(body)]
    return json.loads(subprocess.run(cmd, capture_output=True, text=True, check=True).stdout)

sql = re.sub(r"^--[^\n]*", "", open(sys.argv[1]).read(), flags=re.M)
for stmt in [s.strip() for s in sql.split(";") if s.strip()]:
    print(">>", stmt.splitlines()[0])
    r = api("post", "/api/2.0/sql/statements", {"warehouse_id": WAREHOUSE, "statement": stmt, "wait_timeout": "50s"})
    while r["status"]["state"] in ("PENDING", "RUNNING"):
        time.sleep(3)
        r = api("get", f"/api/2.0/sql/statements/{r['statement_id']}")
    if r["status"]["state"] != "SUCCEEDED":
        sys.exit(f"failed: {r['status'].get('error', {}).get('message')}")
print("done")
