from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

spark = SparkSession.builder.appName("DataCleaningTransactions").getOrCreate()

transactions_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("account_id", StringType(), True),
    StructField("transaction_date", StringType(), True),
    StructField("transaction_amount", DoubleType(), True),
    StructField("transaction_type", StringType(), True)
])

raw_path = "gs://raw_data_bronze/transactions.csv"
transactions_df = spark.read.csv(raw_path, header=True, schema=transactions_schema)

cleaned_transactions_df = transactions_df.dropna(subset=["transaction_id", "account_id"])

curated_path = "gs://curated_data_silver/transactions"
cleaned_transactions_df.write.mode("overwrite").parquet(curated_path)

print("Data cleaning for transactions.csv completed.")
