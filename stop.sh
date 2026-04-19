#!/usr/bin/env bash
# Stop WeatherBot completely.

PID_FILE="$(dirname "$0")/bot.pid"

KILLED=0

# Kill by PID file
if [ -f "$PID_FILE" ]; then
    PID=$(cat "$PID_FILE")
    if kill -0 "$PID" 2>/dev/null; then
        echo "Stopping bot (PID $PID)..."
        kill "$PID" 2>/dev/null
        KILLED=1
    else
        echo "PID $PID not running (stale pid file)"
    fi
    rm -f "$PID_FILE"
fi

# Kill anything on port 5000 (catches any orphaned processes)
# Use netstat since lsof is unavailable on this system
PORT_WPIDS=$(netstat -ano 2>/dev/null | grep "0.0.0.0:5000.*LISTENING" | awk '{print $NF}' | sort -u)
for WPID in $PORT_WPIDS; do
    # Convert Windows PID to bash PID via ps
    BPID=$(ps aux 2>/dev/null | awk -v wpid="$WPID" '$4==wpid || $3==wpid {print $1}' | head -1)
    if [ -n "$BPID" ]; then
        echo "Killing process on port 5000 (PID $BPID / WinPID $WPID)..."
        kill "$BPID" 2>/dev/null || taskkill //F //PID "$WPID" 2>/dev/null || true
        KILLED=1
    fi
done

# Final sweep: kill any lingering run.py processes
RUNPY_PIDS=$(pgrep -f "python.*run\.py" 2>/dev/null || true)
if [ -n "$RUNPY_PIDS" ]; then
    echo "Killing leftover run.py processes: $RUNPY_PIDS"
    kill $RUNPY_PIDS 2>/dev/null || true
    KILLED=1
fi

if [ "$KILLED" -eq 1 ]; then
    sleep 1
    echo "Bot stopped."
else
    echo "No bot process found."
fi
