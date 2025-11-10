#!/bin/bash 

# This file requires the respective trigger containers to be available 
# within the same directory as this file.

TRIGGER_FILE="trigger_on_demand_start.json"
TRIGGER_NAME

# Extract Name field from JSON file
TRIGGER_NAME=$(jq -r '.Name' "${TRIGGER_FILE}")
echo "Processing trigger name: ${TRIGGER_NAME}"

if [ -f "./${TRIGGER_FILE}" ]; then
    echo "Attempting to delete both the daily and on_demand triggers..."
    aws glue delete-trigger --name "trigger_daily_start" 2>/dev/null || true
    aws glue delete-trigger --name "$TRIGGER_NAME" 2>/dev/null || true

    echo "⏳ waiting for trigger deletion to finalize..."
    sleep 30   # 30–60 seconds is usually plenty

    # Create the trigger:
    aws glue create-trigger --cli-input-json file://${TRIGGER_FILE}
else
    echo "Trigger container file (${TRIGGER_FILE}) not found"
fi

echo On Demand start trigger created successfully.
