#!/bin/sh
set -eu
mkdir -p /devstate/pg /run/postgresql
chown -R postgres:postgres /devstate/pg /run/postgresql
if [ ! -f /devstate/pg/PG_VERSION ]; then
  su postgres -s /bin/sh -c 'initdb -D /devstate/pg --auth=trust --no-locale' >/dev/null
fi
su postgres -s /bin/sh -c "pg_ctl -D /devstate/pg -l /devstate/pg/postgres.log -o '-h 127.0.0.1 -p 5432' start"
su postgres -s /bin/sh -c 'createdb fidelity_test' 2>/dev/null || true
touch /devstate/activity
trap 'su postgres -s /bin/sh -c "pg_ctl -D /devstate/pg stop -m fast"; exit' TERM INT
while true; do
  latest=$(/app/backend/.venv/bin/python /work/tools/fidelity-activity.py)
  now=$(date +%s)
  if [ "$((now - ${latest:-$now}))" -gt 86400 ]; then
    su postgres -s /bin/sh -c 'pg_ctl -D /devstate/pg stop -m fast'
    exit 0
  fi
  sleep 60 & wait $! || true
done
