#!/bin/bash 

# This file requires the respective trigger containers to be available within the same 
# directory as this file.

export BRONZE_TO_SILVER_TRIGGER_CONTAINER="trigger_bronze_to_silver.json"
export SILVER_TO_GOLD_TRIGGER_CONTAINER="trigger_silver_to_gold.json"

if [ -f "./${BRONZE_TO_SILVER_TRIGGER_CONTAINER}" ]; then
    # Create the trigger:
    aws glue create-trigger --cli-input-json file://${BRONZE_TO_SILVER_TRIGGER_CONTAINER}
else
    echo "Trigger container file (${BRONZE_TO_SILVER_TRIGGER_CONTAINER}) not found"
fi

if [ -f "./${SILVER_TO_GOLD_TRIGGER_CONTAINER}" ]; then
    # Create the trigger:
    aws glue create-trigger --cli-input-json file://${SILVER_TO_GOLD_TRIGGER_CONTAINER}
else
    echo "Trigger container file (${SILVER_TO_GOLD_TRIGGER_CONTAINER}) not found"
fi

echo Triggers created successfully.
