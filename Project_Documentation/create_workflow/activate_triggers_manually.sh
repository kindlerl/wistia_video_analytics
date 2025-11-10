#!/bin/bash

aws glue start-trigger --name trigger_bronze_to_metadata_silver
aws glue start-trigger --name trigger_metadata_to_fact_silver
aws glue start-trigger --name trigger_silver_to_gold

