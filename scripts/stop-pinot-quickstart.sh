#!/bin/bash

echo "Stopping Pinot cluster..."

# Find all Java processes related to Pinot (exclude this script)
PINOT_PIDS=$(ps aux | grep -i pinot | grep -v grep | grep -v stop-pinot-quickstart | awk '{print $2}')

if [ -z "$PINOT_PIDS" ]; then
    echo "No Pinot processes found running"
    exit 0
fi

echo "Found Pinot processes with PIDs: $PINOT_PIDS"

# Kill each process
for PID in $PINOT_PIDS; do
    # Check if process still exists before trying to kill it
    if kill -0 $PID 2>/dev/null; then
        echo "Killing process $PID..."
        kill $PID 2>/dev/null || {
            echo "Process $PID already terminated"
            continue
        }
        
        # Wait up to 10 seconds for graceful shutdown
        sleep 10
        if kill -0 $PID 2>/dev/null; then
            echo "Process $PID still running, forcing kill..."
            kill -9 $PID 2>/dev/null
        fi
    else
        echo "Process $PID already terminated"
    fi
done

# Wait a moment and verify all processes are stopped
sleep 2
REMAINING_PIDS=$(ps aux | grep -i pinot | grep -v grep | grep -v stop-pinot-quickstart | awk '{print $2}')

if [ -z "$REMAINING_PIDS" ]; then
    echo "Pinot cluster stopped successfully"
else
    echo "Warning: Some Pinot processes may still be running: $REMAINING_PIDS"
    exit 1
fi 