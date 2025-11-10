#!/bin/bash

# Script: create-triggers.sh
# Purpose: Delete existing triggers (if any) and recreate them from JSON definitions.

export BRONZE_TO_METADATA_SILVER_TRIGGER_CONTAINER="trigger_bronze_to_metadata_silver.json"
export METADATA_TO_FACT_SILVER_TRIGGER_CONTAINER="trigger_metadata_to_fact_silver.json"
export SILVER_TO_GOLD_TRIGGER_CONTAINER="trigger_silver_to_gold.json"

# Function to (re)create a trigger
create_trigger() {
    local TRIGGER_FILE=$1
    local TRIGGER_NAME

    # Extract Name field from JSON file
    TRIGGER_NAME=$(jq -r '.Name' "${TRIGGER_FILE}")
    echo "Processing trigger name: ${TRIGGER_NAME}"

    if [ -z "$TRIGGER_NAME" ] || [ "$TRIGGER_NAME" == "null" ]; then
        echo "❌ Could not extract trigger name from ${TRIGGER_FILE}"
        return 1
    fi

    echo "🧹 Deleting old trigger (if exists): ${TRIGGER_NAME}"
    aws glue delete-trigger --name "${TRIGGER_NAME}" 2>/dev/null || true

    echo "⏳ waiting for trigger deletion to finalize..."
    sleep 60   # 30–60 seconds is usually plenty

    echo "🚀 Creating trigger: ${TRIGGER_NAME}"
    aws glue create-trigger --cli-input-json file://"${TRIGGER_FILE}"
    aws glue start-trigger --name "${TRIGGER_NAME}"
}

# Create triggers
if [ -f "./${BRONZE_TO_METADATA_SILVER_TRIGGER_CONTAINER}" ]; then
    create_trigger "./${BRONZE_TO_METADATA_SILVER_TRIGGER_CONTAINER}"
else
    echo "⚠️ Missing ${BRONZE_TO_METADATA_SILVER_TRIGGER_CONTAINER}"
fi

if [ -f "./${METADATA_TO_FACT_SILVER_TRIGGER_CONTAINER}" ]; then
    create_trigger "./${METADATA_TO_FACT_SILVER_TRIGGER_CONTAINER}"
else
    echo "⚠️ Missing ${METADATA_TO_FACT_SILVER_TRIGGER_CONTAINER}"
fi

if [ -f "./${SILVER_TO_GOLD_TRIGGER_CONTAINER}" ]; then
    create_trigger "./${SILVER_TO_GOLD_TRIGGER_CONTAINER}"
else
    echo "⚠️ Missing ${SILVER_TO_GOLD_TRIGGER_CONTAINER}"
fi

echo "✅ Triggers created or updated successfully."

