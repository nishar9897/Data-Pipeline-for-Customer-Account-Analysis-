# Upsert & Load to BigQuery

## Purpose
This step performs the final load of processed customer account balance data into a BigQuery table. It uses a merge (upsert) strategy to ensure both new and updated records are properly handled.

## Input
- Bucket: `gs://your-bucket/agg-refined/`
- File: `customer_balance.parquet`
- Marker file: `trigger_etl_<date>.txt`

## BigQuery Tables
- Target Table: `customer_account_balance`
- Staging Table: `refined_gold_customer_balance_temp`

## Process

### 1. One-Time Table Creation
Script: `create_customer_balance_table.sh`

- Creates the BigQuery table with schema:
  - customer_id (STRING)
  - account_id (STRING)
  - balance (NUMERIC)
  - last_updated (TIMESTAMP)
- Should be run only once before loading begins.

### 2. Daily Upsert
Script: `daily_customer_balance_upsert.sh`

- Loads the Parquet file into a staging table.
- Executes a `MERGE` statement to upsert data from staging into the target table.
- Ensures:
  - Existing customer records are updated.
  - New customers are inserted.
  - `last_updated` field is refreshed with `CURRENT_TIMESTAMP()`.

### 3. Trigger Logic
- A Cloud Function monitors the `agg-refined/` bucket.
- It runs only when:
  1. `customer_balance.parquet` is modified today.
  2. `trigger_etl_<date>.txt` is present in the same bucket.
- This prevents early execution and ensures only complete daily output is processed.

## Output
- Updated BigQuery table: `customer_account_balance` with fresh balances.

## Automation Logic
- This stage is part of an event-driven pipeline.
- Triggered by Cloud Storage file creation events.
- Controlled using marker files and metadata validation to ensure freshness.
- Designed to work in append mode without relying on folder separation.

## Notes
- Cloud Scheduler (optional): Can be used to trigger upserts at a fixed daily time.
- Make sure service account used by Cloud Function has proper BigQuery access.
- Marker files used to ensure dependency between ETL and load stages.

