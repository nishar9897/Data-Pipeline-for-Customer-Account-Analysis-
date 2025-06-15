# ETL Pipeline (Silver to Gold)

## Purpose
This script performs ETL operations on cleaned datasets, generating aggregated metrics required for reporting and analytics.

## Input
- Bucket: `gs://your-bucket/curated/`
- Files:
  - accounts.parquet
  - customers.parquet
- Marker file: `trigger_cleaned_<date>.txt`

## Process
- Triggered by file creation in the `curated/` bucket.
- Executes only when all of the following conditions are met:
  1. accounts.parquet and customers.parquet exist
  2. Both files have a last-modified date equal to today's date
  3. The marker file `trigger_cleaned_<date>.txt` exists

- Transformations include:
  - Join on customer_id between accounts and customers
  - Calculation of total account balance per customer
  - Selection of all fields from both datasets
  - Addition of last_updated timestamp column

- Output is written to: `gs://your-bucket/agg-refined/`

## Output
- Aggregated Parquet file:
  - customer_balance.parquet
- Marker file: `trigger_etl_<date>.txt`

## Automation Logic
- The ETL function is triggered automatically when files are added to the curated bucket.
- The function includes logic to verify input freshness and completion using both file metadata and a trigger marker file.
- This ensures the ETL job only runs after the cleaning stage has completed successfully.

## Notes
- This pipeline design uses append mode.
- Marker files are used to establish stage-to-stage dependencies, ensuring each step processes only valid and complete data.

