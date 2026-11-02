#!/usr/bin/env bash
# A local single-node RabbitMQ for Orderly, loopback only, separate from any other RabbitMQ on this machine.
#
#   scripts/rabbitmq.sh start | stop | status | url | logs [n] | reset
#
# Data, logs and config live in .rabbitmq/ (git-ignored). AMQP on 127.0.0.1:5673, Erlang distribution on 25673,
# node name orderly@localhost (the portfolio's port map). Overrides: ORDERLY_RABBITMQ_DIR, ORDERLY_RABBITMQ_PORT,
# ORDERLY_RABBITMQ_NODE, RABBITMQ_SBIN, EPMD. Homebrew's own rabbitmq-env.conf and enabled_plugins (management, MQTT, STOMP,
# streams: more ports) are not used. `reset` deletes the directory, but only one holding the script's marker. `stop`
# signals only a process holding this directory's log file open. Erlang's port mapper (epmd, port 4369), which the
# node starts if none is running, is bound to loopback here and stopped with the node when no other node uses it.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DIR="${ORDERLY_RABBITMQ_DIR:-$ROOT/.rabbitmq}"
case "$DIR" in /*) ;; *) DIR="$PWD/$DIR" ;; esac
PORT="${ORDERLY_RABBITMQ_PORT:-5673}"
DIST_PORT=$((PORT + 20000))
NODE="${ORDERLY_RABBITMQ_NODE:-orderly@localhost}"
SBIN="${RABBITMQ_SBIN:-/opt/homebrew/opt/rabbitmq/sbin}"
START_TIMEOUT="${ORDERLY_RABBITMQ_START_TIMEOUT:-60}"

# Everything below is derived from DIR. set_paths makes DIR physical first (no symlinks, no ./ or ..), so the path on
# the node's command line, which running_pid looks for, is the same however the directory was spelled (/tmp is
# /private/tmp on macOS).
set_paths() {
  if [ -d "$DIR" ]; then DIR="$(cd "$DIR" && pwd -P)"; fi
  MARKER="$DIR/.orderly-rabbitmq"
  PIDFILE="$DIR/rabbitmq.pid"
  export RABBITMQ_NODENAME="$NODE"
  export RABBITMQ_NODE_IP_ADDRESS=127.0.0.1
  export RABBITMQ_NODE_PORT="$PORT"
  export RABBITMQ_DIST_PORT="$DIST_PORT"
  export RABBITMQ_MNESIA_BASE="$DIR/mnesia"
  export RABBITMQ_LOG_BASE="$DIR/log"
  export RABBITMQ_PID_FILE="$PIDFILE"
  export RABBITMQ_CONF_ENV_FILE="$DIR/rabbitmq-env.conf"
  export RABBITMQ_CONFIG_FILE="$DIR/rabbitmq.conf"
  export RABBITMQ_ENABLED_PLUGINS_FILE="$DIR/enabled_plugins"
  # Erlang distribution (25673) and epmd on loopback too; both listen on every interface by default.
  export RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS="-kernel inet_dist_use_interface {127,0,0,1}"
  export ERL_EPMD_ADDRESS=127.0.0.1
}
set_paths

die() { echo "rabbitmq.sh: $*" >&2; exit 1; }

# The pid in the pid file (the node writes it), if that process is this directory's node. A pid is reused once its
# process exits (a crash, a reboot), so a live pid alone proves nothing. RabbitMQ 4 reads its directories from the
# environment, not the command line, so the proof is the node's own log file in this directory, which it keeps open.
running_pid() {
  [ -f "$PIDFILE" ] || return 1
  local pid
  pid="$(cat "$PIDFILE")"
  case "$pid" in '' | *[!0-9]* | 0) return 1 ;; esac
  kill -0 "$pid" 2>/dev/null || return 1
  lsof -p "$pid" -Fn 2>/dev/null | grep -qxF -- "n$DIR/log/$NODE.log" || return 1
  echo "$pid"
}

# epmd outlives the node that started it. Stop it once no node is registered with it any more.
stop_idle_epmd() {
  local epmd="" candidate
  # Homebrew's wrapper puts its Erlang on PATH only for the server, so look where Homebrew and Debian put epmd.
  for candidate in "${EPMD:-}" "$(command -v epmd || true)" /opt/homebrew/opt/erlang@28/bin/epmd \
    /opt/homebrew/opt/erlang/bin/epmd /usr/lib/erlang/bin/epmd /usr/bin/epmd; do
    if [ -n "$candidate" ] && [ -x "$candidate" ]; then epmd="$candidate"; break; fi
  done
  [ -n "$epmd" ] || return 0
  if "$epmd" -names 2>/dev/null | grep -q '^name '; then return 0; fi
  "$epmd" -kill > /dev/null 2>&1 || true
}

# A connect, not lsof: it needs no tool, and on macOS it also sees a listener on *:port, which can coexist with a
# bind to 127.0.0.1:port.
listening() { (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null; }

write_config() {
  echo "# Written by scripts/rabbitmq.sh: keeps Homebrew's rabbitmq-env.conf out." > "$DIR/rabbitmq-env.conf"
  # No plugins: the management UI and the other protocols would each take another port.
  echo "[]." > "$DIR/enabled_plugins"
  cat > "$DIR/rabbitmq.conf" <<EOF
# Written by scripts/rabbitmq.sh on every start; edit the script, not this file.
listeners.tcp.1 = 127.0.0.1:$PORT
# guest/guest, which RabbitMQ allows only from loopback (the default).
loopback_users.guest = true
log.console = false
log.file.level = info
EOF
}

start() {
  if pid="$(running_pid)"; then
    echo "rabbitmq already running (pid $pid) on 127.0.0.1:$PORT"
    return 0
  fi
  for port in "$PORT" "$DIST_PORT"; do
    listening "$port" && die "something else is listening on port $port"
  done
  [ -x "$SBIN/rabbitmq-server" ] || die "no rabbitmq-server in $SBIN (brew install rabbitmq, or set RABBITMQ_SBIN)"
  if [ -e "$DIR" ] && [ ! -f "$MARKER" ] && [ -n "$(ls -A "$DIR")" ]; then
    die "$DIR exists and wasn't made by this script"
  fi
  mkdir -p "$DIR/log"
  set_paths  # the directory exists now, so this is its physical path: the one the node is started with
  touch "$MARKER"
  write_config
  rm -f "$PIDFILE"
  nohup "$SBIN/rabbitmq-server" > "$DIR/log/console.log" 2>&1 &
  local child=$!
  local deadline=$((SECONDS + START_TIMEOUT))
  # The node writes the pid file early and the AMQP listener opens last; wait for the listener.
  until listening "$PORT"; do
    if ! kill -0 "$child" 2>/dev/null; then
      tail -20 "$DIR/log/console.log" >&2
      die "rabbitmq exited during startup (log: $DIR/log)"
    fi
    if [ "$SECONDS" -ge "$deadline" ]; then
      tail -20 "$DIR/log/console.log" >&2
      die "rabbitmq didn't open port $PORT within ${START_TIMEOUT}s (still running: scripts/rabbitmq.sh stop)"
    fi
    sleep 0.5
  done
  pid="$(running_pid)" || die "port $PORT is open but $PIDFILE doesn't name this directory's node"
  echo "rabbitmq running (pid $pid) on 127.0.0.1:$PORT"
}

stop() {
  local pid
  if ! pid="$(running_pid)"; then
    local recorded
    recorded="$(cat "$PIDFILE" 2>/dev/null || true)"
    case "$recorded" in '' | *[!0-9]* | 0) ;; *)
      if kill -0 "$recorded" 2>/dev/null; then
        # Alive but not provably this directory's node: neither signal it nor drop the only record of it.
        echo "rabbitmq.sh: pid $recorded in $PIDFILE is alive but wasn't started on $DIR/mnesia;" \
          "leaving it and the pid file alone" >&2
        return 1
      fi ;;
    esac
    rm -f "$PIDFILE"
    echo "rabbitmq not running"
    return 0
  fi
  # RabbitMQ shuts down cleanly on SIGTERM (it closes connections and flushes queues first).
  kill "$pid" 2>/dev/null || true  # it may have exited since running_pid looked
  for _ in $(seq 1 120); do
    kill -0 "$pid" 2>/dev/null || break
    sleep 0.5
  done
  if kill -0 "$pid" 2>/dev/null; then
    echo "rabbitmq.sh: pid $pid still running after 60s; killing it" >&2
    kill -9 "$pid" 2>/dev/null || true
  fi
  rm -f "$PIDFILE"
  stop_idle_epmd
  echo "rabbitmq stopped"
}

status() {
  local pid
  if pid="$(running_pid)"; then
    echo "running (pid $pid) on 127.0.0.1:$PORT, data in $DIR/mnesia"
  else
    echo "stopped"
    return 3
  fi
}

reset() {
  stop
  [ -e "$DIR" ] || { echo "nothing to reset"; return 0; }
  [ -f "$MARKER" ] || die "$DIR wasn't made by this script; not deleting it"
  rm -rf "$DIR"
  echo "deleted $DIR"
}

case "${1:-}" in
  start) start ;;
  stop) stop ;;
  status) status ;;
  url) echo "amqp://guest:guest@127.0.0.1:$PORT/" ;;
  logs) tail -n "${2:-50}" "$DIR/log/$NODE.log" ;;
  reset) reset ;;
  *) echo "usage: scripts/rabbitmq.sh start|stop|status|url|logs [n]|reset" >&2; exit 2 ;;
esac
