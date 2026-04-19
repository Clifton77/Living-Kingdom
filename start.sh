#!/usr/bin/env bash
# Start WeatherBot. Kills any existing instance first.
set -e

PID_FILE="$(dirname "$0")/bot.pid"

# Kill any existing bot processes
if [ -f "$PID_FILE" ]; then
    OLD_PID=$(cat "$PID_FILE")
    if kill -0 "$OLD_PID" 2>/dev/null; then
        echo "Stopping existing bot (PID $OLD_PID)..."
        kill "$OLD_PID" 2>/dev/null
        sleep 2
    fi
    rm -f "$PID_FILE"
fi

# Kill anything still on port 5000 (catches orphaned processes)
PORT_WPIDS=$(netstat -ano 2>/dev/null | grep "0.0.0.0:5000.*LISTENING" | awk '{print $NF}' | sort -u)
for WPID in $PORT_WPIDS; do
    BPID=$(ps aux 2>/dev/null | awk -v wpid="$WPID" '$4==wpid || $3==wpid {print $1}' | head -1)
    if [ -n "$BPID" ]; then
        echo "Killing orphaned process on port 5000 (PID $BPID / WinPID $WPID)..."
        kill "$BPID" 2>/dev/null || taskkill //F //PID "$WPID" 2>/dev/null || true
    fi
done
sleep 1

# Start bot and save PID
cd "$(dirname "$0")"
python run.py &
BOT_PID=$!
echo "$BOT_PID" > "$PID_FILE"
echo "WeatherBot started (PID $BOT_PID) — dashboard at http://localhost:5000"
