#!/bin/bash

if [ $# -eq 0 ]; then
    echo "ERROR: Please include the run-id as a parameter"
else
    aws glue get-workflow-run --name wistia_daily_workflow --run-id $1 
fi

