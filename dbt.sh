#!/usr/bin/env bash
# runs dbt with the token from your databricks cli profile (~/.databrickscfg), so it never sits in a file
# usage: ./dbt.sh build   (any dbt command)
set -euo pipefail
export DATABRICKS_TOKEN=$(python3 -c "
import configparser, os
c = configparser.ConfigParser(); c.read(os.path.expanduser('~/.databrickscfg'))
print(c[os.environ.get('DATABRICKS_PROFILE', 'DEFAULT')]['token'])")
cd "$(dirname "$0")/dbt"
exec ../.venv/bin/dbt "$@"
