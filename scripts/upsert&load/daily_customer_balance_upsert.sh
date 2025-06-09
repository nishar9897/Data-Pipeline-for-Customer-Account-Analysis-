#!/bin/bash

# Set environment variables
PROJECT_ID="de-practices"
DATASET="customer_data"
TARGET_TABLE="customer_account_balance"
STAGING_TABLE="refined_gold_customer_balance_temp"

# Load Parquet data from GCS into staging table
echo "Loading Parquet data into staging table..."
bq load --autodetect --source_format=PARQUET \
    ${DATASET}.${STAGING_TABLE} \
    gs://aggregated_data_gold/customer_account_balance/*.parquet

# Perform the upsert
echo "Performing daily upsert from ${STAGING_TABLE} to ${TARGET_TABLE}..."

bq query --use_legacy_sql=false --project_id="${PROJECT_ID}" <<EOF
MERGE \`${PROJECT_ID}.${DATASET}.${TARGET_TABLE}\` T
USING \`${PROJECT_ID}.${DATASET}.${STAGING_TABLE}\` S
ON T.customer_id = S.customer_id
WHEN MATCHED THEN
  UPDATE SET 
    first_name = S.first_name,
    last_name = S.last_name,
    address = S.address,
    city = S.city,
    state = S.state,
    zip = S.zip,
    total_balance = S.total_balance,
    last_updated = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
  INSERT (customer_id, first_name, last_name, address, city, state, zip, total_balance, last_updated)
  VALUES (S.customer_id, S.first_name, S.last_name, S.address, S.city, S.state, S.zip, S.total_balance, CURRENT_TIMESTAMP());
EOF

echo "Daily upsert completed."
