#!/bin/bash 

# This file requires the respective trigger containers to be available 
# within the same directory as this file.

export DAILY_START_TRIGGER="trigger_daily_start.json"

if [ -f "./${DAILY_START_TRIGGER}" ]; then
    # Create the trigger:
    aws glue create-trigger --cli-input-json file://${DAILY_START_TRIGGER}
else
    echo "Trigger container file (${DAILY_START_TRIGGER}) not found"
fi

echo Daily start trigger created successfully.
