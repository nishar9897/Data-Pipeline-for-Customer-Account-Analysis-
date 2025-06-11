# GCP Data Engineering Pipeline

This project implements an end-to-end data engineering pipeline using Google Cloud Platform (GCP) services, including Cloud Scheduler and Cloud Composer (Airflow), to automate and manage various tasks.

---

## Project Overview

The pipeline processes raw CSV files, cleans and transforms them using PySpark on Dataproc, aggregates the data into Parquet format, and finally loads the data into BigQuery. Automation ensures seamless, consistent, and reliable data processing.

---

## Implementation and Integration

### 1. Cloud Scheduler for File Copy Automation

In this project, I implemented Cloud Scheduler to automate the copying of aggregated Parquet files from the aggregated_data_gold bucket to a designated destination bucket for downstream processing.

- I wrote a Cloud Function that uses the Google Cloud Storage client to copy files from `aggregated_data_gold/customer_account_balance/` to the destination bucket.
- I deployed this function using the GCP Console.
- I configured Cloud Scheduler to run the function daily at midnight, ensuring the aggregated data is always available in the destination bucket for further tasks.

---

### 2. Cloud Composer (Airflow) for Full Pipeline Automation

To orchestrate the complete data pipeline, I set up Cloud Composer (managed Airflow). This allowed me to manage complex workflows with task dependencies and monitoring.

- I wrote an Airflow DAG that included the following tasks:
  - Cleaning raw CSV files with Dataproc PySpark jobs (accounts, customers, loans, transactions).
  - Aggregating cleaned data into Parquet format in the aggregated_data_gold bucket.
  - Loading the aggregated data from GCS into BigQuery with an upsert process.
- I deployed the DAG file to the Composer environment’s DAGs folder.
- I configured the DAG’s schedule and set up task dependencies to ensure each step runs in the correct order.
- I used the Airflow UI to monitor runs, view logs, set retries, and configure alerts for failures.

---

## Difference Between Cloud Scheduler and Cloud Composer

In this project, I utilized both Cloud Scheduler and Cloud Composer based on their strengths:

| Feature               | Cloud Scheduler              | Cloud Composer               |
|-----------------------|------------------------------|------------------------------|
| Purpose               | Simple, time-based triggers  | Complex, multi-step workflows |
| Use Cases             | Trigger a Cloud Function to copy files | Orchestrate the entire data pipeline end-to-end |
| Management            | Lightweight and easy to set up | Managed Airflow environment with advanced features |
| Task Chaining         | Not natively supported       | Supports dependencies between tasks |
| Monitoring            | Basic logs and status        | Detailed DAG visualization, retries, and alerting |
| Cost                  | Lower                        | Higher (Airflow clusters)    |

---

## Summary

By integrating Cloud Scheduler for lightweight automation and Cloud Composer for complex workflows, I ensured that both simple tasks and end-to-end data pipeline processes are automated and reliable.

This approach allows for scalability, monitoring, and error handling, making the pipeline robust and production-ready.

---

## Project Structure

