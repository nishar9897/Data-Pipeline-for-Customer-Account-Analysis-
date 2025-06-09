#!/bin/bash

# Set environment variables
PROJECT_ID="de-practices"
DATASET="customer_data"
TABLE_NAME="customer_account_balance"

echo "Creating BigQuery table: ${DATASET}.${TABLE_NAME}..."

bq query --use_legacy_sql=false --project_id="${PROJECT_ID}" <<EOF
CREATE TABLE IF NOT EXISTS \`${PROJECT_ID}.${DATASET}.${TABLE_NAME}\` (
  customer_id STRING,
  first_name STRING,
  last_name STRING,
  address STRING,
  city STRING,
  state STRING,
  zip STRING,
  total_balance NUMERIC,
  last_updated TIMESTAMP
)
OPTIONS(
  description="Customer Account Balance Table",
  labels=[("env", "prod")]
);
EOF

echo "Table creation completed."
