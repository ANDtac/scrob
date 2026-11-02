#!/bin/sh
set -eu
test "$DATABASE_URL" = 'postgresql+asyncpg://postgres@127.0.0.1/fidelity_test'
touch /devstate/activity
if [ ! -f /devstate/backend.pid ] || ! kill -0 "$(cat /devstate/backend.pid)" 2>/dev/null; then
  cd /work/backend
  nohup /app/backend/.venv/bin/uvicorn main:app --host 127.0.0.1 --port 7331 >/devstate/backend.log 2>&1 &
  echo $! >/devstate/backend.pid
fi
if [ ! -f /devstate/frontend.pid ] || ! kill -0 "$(cat /devstate/frontend.pid)" 2>/dev/null; then
  cd /work/frontend
  HOST=0.0.0.0 PORT=7330 BACKEND_PORT=7331 nohup node ./dist/server/entry.mjs >/devstate/frontend.log 2>&1 &
  echo $! >/devstate/frontend.pid
fi
