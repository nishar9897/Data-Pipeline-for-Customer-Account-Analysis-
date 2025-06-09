from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as sum_

# 1. Start Spark Session
spark = SparkSession.builder.appName("AggregateCustomerBalance").getOrCreate()

# 2. Read customers data
customers_df = spark.read.parquet("gs://curated_data_silver/customers/")

# 3. Read accounts data
accounts_df = spark.read.parquet("gs://curated_data_silver/accounts/")

# 4. Join datasets on customer_id
joined_df = accounts_df.join(customers_df, on="customer_id", how="inner")

# 5. Aggregate: compute total balance per customer
aggregated_df = joined_df.groupBy("customer_id", "first_name", "last_name", "address", "city", "state", "zip") \
    .agg(sum_("balance").alias("total_balance"))

# 6. Write aggregated data to gold bucket
output_path = "gs://aggregated_data_gold/customer_account_balance"
aggregated_df.write.mode("overwrite").parquet(output_path)

print("Aggregation completed. Output saved to agg-refined bucket.")
