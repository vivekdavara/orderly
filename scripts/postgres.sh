#!/usr/bin/env bash
# Orderly's databases on the local PostgreSQL 14 server (the one Homebrew runs on localhost:5432).
#
#   scripts/postgres.sh create | migrate | status | drop
#
# create: makes the service databases (orderly_orders, orderly_payments, orderly_notifications) and the test database
#   (orderly_test) if missing, then migrates the service databases.
# migrate: applies pending migrations to each service database (python -m orderly.migrate).
# status: lists the databases and each one's latest applied migration.
# drop: drops exactly those databases, after asking (ORDERLY_PG_YES=1 to skip the question).
#
# Connection: libpq's own variables (PGHOST, PGPORT, PGUSER, PGPASSWORD); defaults localhost:5432 as the current
# user. ORDERLY_PG_PREFIX (default orderly) names the databases <prefix>_orders, ...: the script's tests use their own. Unlike the RabbitMQ script this starts no server: the PostgreSQL 14 server is shared by the portfolio's
# projects, each with its own databases.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
export PGHOST="${PGHOST:-localhost}"
export PGPORT="${PGPORT:-5432}"
PSQL="${PSQL:-psql}"
PYTHON="${PYTHON:-$ROOT/.venv/bin/python}"
SERVICES=(orders payments notifications)
PREFIX="${ORDERLY_PG_PREFIX:-orderly}"
TEST_DB="${PREFIX}_test"

die() { echo "postgres.sh: $*" >&2; exit 1; }

# The names go into SQL unquoted, so the prefix must be a plain lowercase identifier.
[[ "$PREFIX" =~ ^[a-z][a-z0-9_]{0,40}$ ]] || die "ORDERLY_PG_PREFIX must match [a-z][a-z0-9_]*, got '$PREFIX'"

q() { "$PSQL" -X -q -v ON_ERROR_STOP=1 -d postgres -Atc "$1"; }

exists() { [ "$(q "SELECT 1 FROM pg_database WHERE datname = '$1'")" = 1 ]; }

server_up() {
  q "SELECT 1" > /dev/null 2>&1 || die "no PostgreSQL at $PGHOST:$PGPORT (brew services start postgresql@14)"
}

all_databases() {
  local service
  for service in "${SERVICES[@]}"; do echo "${PREFIX}_$service"; done
  echo "$TEST_DB"
}

create() {
  server_up
  local db
  for db in $(all_databases); do
    if exists "$db"; then
      echo "$db exists"
    else
      # Not `q ... && echo`: set -e ignores a failure on the left of &&, and the script exited 0 after one.
      q "CREATE DATABASE $db" || die "could not create $db"
      echo "$db created"
    fi
  done
  migrate
}

migrate() {
  server_up
  # A path (the default) or a command name (CI sets PYTHON=python): command -v resolves both.
  local python
  python="$(command -v "$PYTHON" || true)"
  [ -n "$python" ] || die "no $PYTHON (make venv, or set PYTHON)"
  local service
  for service in "${SERVICES[@]}"; do
    "$python" -m orderly.migrate "$service" --url "postgresql:///${PREFIX}_$service"
  done
}

status() {
  server_up
  local db version
  for db in $(all_databases); do
    if ! exists "$db"; then
      echo "$db: missing"
    elif [ "$db" = "$TEST_DB" ]; then
      echo "$db: present (tests make a schema each)"
    else
      version="$("$PSQL" -X -q -d "$db" -Atc \
        "SELECT coalesce(max(version)::text, 'none') FROM schema_migrations" 2>/dev/null || echo "none")"
      echo "$db: migration $version"
    fi
  done
}

drop() {
  server_up
  local databases
  databases="$(all_databases | tr '\n' ' ')"
  if [ "${ORDERLY_PG_YES:-}" != 1 ]; then
    printf 'Drop %son %s:%s? [y/N] ' "$databases" "$PGHOST" "$PGPORT"
    read -r answer
    [ "$answer" = y ] || [ "$answer" = Y ] || { echo "nothing dropped"; return 0; }
  fi
  local db failed=0
  for db in $databases; do
    if exists "$db"; then
      # WITH (FORCE) ends the services' and tests' open connections instead of failing on them (PostgreSQL 13+).
      # A failure doesn't stop the others being dropped, but the script exits 1 naming it.
      if q "DROP DATABASE $db WITH (FORCE)"; then
        echo "$db dropped"
      else
        echo "postgres.sh: could not drop $db" >&2
        failed=1
      fi
    fi
  done
  return "$failed"
}

case "${1:-}" in
  create) create ;;
  migrate) migrate ;;
  status) status ;;
  drop) drop ;;
  *) echo "usage: scripts/postgres.sh create|migrate|status|drop" >&2; exit 2 ;;
esac
