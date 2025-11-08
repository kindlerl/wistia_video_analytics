#!/bin/bash 

# This file requires the workflow container to be available within the same 
# directory as this file.

export WORKFLOW_CONTAINER="wistia_daily_workflow.json"

if [ -f "./${WORKFLOW_CONTAINER}" ]; then
    # Create the workflow:
    aws glue create-workflow --cli-input-json file://wistia_daily_workflow.json
    #echo "File ${WORKFLOW_CONTAINER} exists!"
else
    echo "Workflow container file (${WORKFLOW_CONTAINER}) not found"
fi
