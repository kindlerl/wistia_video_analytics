#!/bin/bash

aws glue get-trigger --name trigger_bronze_to_metadata_silver | grep State
aws glue get-trigger --name trigger_metadata_to_fact_silver | grep State
aws glue get-trigger --name trigger_silver_to_gold | grep State

