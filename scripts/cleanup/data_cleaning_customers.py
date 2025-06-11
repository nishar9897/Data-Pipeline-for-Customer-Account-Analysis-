from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

spark = SparkSession.builder.appName("DataCleaningCustomers").getOrCreate()

customers_schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("zip", StringType(), True)
])

raw_path = "gs://raw_data_bronze/customers.csv"
customers_df = spark.read.csv(raw_path, header=True, schema=customers_schema)

cleaned_customers_df = customers_df.dropna(subset=["customer_id"])

curated_path = "gs://curated_data_silver/customers"
cleaned_customers_df.write.mode("append").parquet(curated_path)

print("Data cleaning for customers.csv completed.")
