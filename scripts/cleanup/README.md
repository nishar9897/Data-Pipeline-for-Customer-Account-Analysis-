# Data Cleaning Pipeline (Bronze to Silver)

## Purpose
This script handles data cleaning and transformation of raw ingested files into a curated format, ready for ETL processing.

## Input
- Bucket: `gs://your-bucket/raw/`
- Files:
  - accounts.csv
  - customers.csv
  - loans.csv
  - loan_payments.csv
  - transactions.csv
- Marker file: `trigger_ingestion_<date>.txt`

## Process
- Triggered by file creation in the `raw/` bucket.
- Executes only when all of the following conditions are met:
  1. All 5 required CSV files exist in the `raw/` bucket.
  2. Each file has a last-modified date equal to today's date.
  3. The marker file `trigger_ingestion_<date>.txt` exists.

- Transformation includes:
  - Null value handling
  - Schema enforcement (no inferSchema)
  - Data type conversion
  - Removal of inconsistent records
  - Writing to Parquet format

- Output is written to: `gs://your-bucket/curated/`

## Output
- Cleaned Parquet files:
  - accounts.parquet
  - customers.parquet
  - loans.parquet
  - loan_payments.parquet
  - transactions.parquet
- Marker file: `trigger_cleaned_<date>.txt`

## Automation Logic
- The pipeline is event-driven.
- The Cloud Function is triggered by any file landing in the raw bucket but guards execution with checks.
- The presence of the trigger file and metadata verification ensures execution happens only when the full set of current-day files is present.

## Notes
- Processing is done in append mode.
- This design ensures reliability and prevents early or duplicate executions.




### Challenges Faced

Initially, I used `.write.mode("overwrite").parquet(...)` when writing Parquet files, which inadvertently caused previous data to be lost whenever the pipeline ran. I fixed this issue by modifying the code to use `.write.mode("append").parquet(...)` instead, ensuring that new data is added without deleting the existing data.
