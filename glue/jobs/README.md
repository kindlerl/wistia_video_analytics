# AWS Glue Job Scripts — Wistia Video Analytics Pipeline

This folder contains all AWS Glue ETL and Python Shell scripts used in the **Wistia Video Analytics** pipeline.  
The pipeline follows a Medallion Architecture (Bronze → Silver → Gold) to ingest, transform, and aggregate Wistia API data for reporting and visualization.

---

## 🥉 Bronze Layer — Ingestion

### `wistia_ingestion_bronze_job.py`
- **Type:** AWS Glue Python Shell Job  
- **Purpose:** Ingests data from the Wistia Stats and Medias APIs.  
- **Logic:**
  - Authenticates using a Wistia API token.
  - Fetches engagement metrics (`/medias/{id}/stats.json`) and metadata (`/medias.json`).
  - Flattens nested JSON for consistency.
  - Writes daily JSON snapshots to S3 in the **bronze/** prefix.
  - Supports incremental ingestion and date-based naming.

---

## 🥈 Silver Layer — Transformation

### `wistia_transform_fact_silver_job.py`
- **Type:** AWS Glue Spark ETL Job  
- **Purpose:** Transforms and structures engagement metrics from the Bronze layer.  
- **Logic:**
  - Reads flattened stats JSON from S3.
  - Enforces schema and converts data to Parquet.
  - Adds `load_date` partition for incremental runs.
  - Registers the `fact_media_engagement` table in the Glue Catalog for Athena.

### `wistia_transform_metadata_silver_job.py`
- **Type:** AWS Glue Spark ETL Job  
- **Purpose:** Cleans and models media-level metadata from the Bronze layer.  
- **Logic:**
  - Reads `media_metadata` JSON files.
  - Extracts relevant attributes (e.g., `id`, `hashed_id`, `name`, `duration`, `created`, `updated`, `project`).
  - Outputs Parquet data to S3 under the `silver/dim_media_metadata` prefix.
  - Registers the `dim_media_metadata` table in the Glue Catalog.

---

## 🥇 Gold Layer — Aggregation

### `wistia_transform_gold_job.py`
- **Type:** AWS Glue Spark ETL Job  
- **Purpose:** Creates aggregated business metrics for analytics and visualization.  
- **Logic:**
  - Reads Silver fact and dimension tables.
  - Joins and aggregates by media, project, and load_date.
  - Writes results to `gold/media_metrics_summary`, partitioned by date.
  - Auto-registers in Glue Catalog as `wistia_gold.media_metrics_summary`.

---

## 🧰 Utility (Optional)

### `wistia_import_historical_data_job.py`
- **Type:** AWS Glue Spark ETL Job  
- **Purpose:** Imports or backfills historical datasets prior to production runs.  
- **Usage:** Run once to populate baseline data, then deactivate for daily workflow.

---

## 🔄 Workflow Integration

These jobs are orchestrated by an **AWS Glue Workflow** named: **wistia_daily_workflow**

