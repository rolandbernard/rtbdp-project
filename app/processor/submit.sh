#!/bin/bash
# This is a small entry script used for the submitting the job to the Flink
# cluster. If the same job is already running on the Flink cluster, a savepoint
# is created, and the job restarted from this savepoint using the new JAR. We use
# the Flink Rest API to interact with the job manager.

JOB_NAME="${JOB_NAME:-GitHub Event Analysis}"
JM_ADDRESS="${JM_ADDRESS:-flink-jobmanager:8081}"
CHECKPOINT_DIR="${CHECKPOINT_DIR:-/opt/flink/checkpoints}"
SAVEPOINT_DIR="${SAVEPOINT_DIR:-/opt/flink/savepoints}"

# Check if this job may already be running. If it is, get the job id.
JOB_ID=$(flink list -m $JM_ADDRESS 2>/dev/null | grep "GitHub Event Analysis" | cut -d" " -f4)

if [ -n "$JOB_ID" ]; then
    echo "Found running job $JOB_ID. Triggering savepoint..."
    
    # Create a savepoint of the currently running process.
    RESTORE_PATH=$(flink stop -m $JM_ADDRESS $JOB_ID | grep "Path: " | cut -d" " -f4)
else
    echo "No running job found. Searching for a checkpoint..."

    # Find the latest checkpoint in the checkpoint directory.
    RESTORE_PATH=$(find $CHECKPOINT_DIR -name "_metadata" -printf '%T@ %p\n' | sort -n | tail -1 | cut -f2- -d" ")
fi

# If we found a restore path, use it, otherwise submit a fresh job.
if [ -n "$RESTORE_PATH" ] && [ "$RESTORE_PATH" != "null" ]; then
    echo "--- Restoring from: $RESTORE_PATH ---"
    RESTORE_ARGS="-s $RESTORE_PATH"
else
    echo "--- Starting fresh (no state found) ---"
    RESTORE_ARGS=""
fi

# Execute the flink run command with the original arguments
flink run -m $JM_ADDRESS -d $RESTORE_ARGS $@
