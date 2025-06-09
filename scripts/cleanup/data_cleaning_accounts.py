from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# 1. Start Spark Session
spark = SparkSession.builder.appName("DataCleaningAccounts").getOrCreate()

# 2. Define the schema for accounts.csv
accounts_schema = StructType([
    StructField("account_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("account_type", StringType(), True),
    StructField("balance", DoubleType(), True)
])

# 3. Read CSV file from raw bucket
raw_path = "gs://raw_data_bronze/accounts.csv"
accounts_df = spark.read.csv(
    raw_path,
    header=True,
    schema=accounts_schema
)

# 4. Clean the data (e.g., drop rows with missing account_id or customer_id)
cleaned_accounts_df = accounts_df.dropna(subset=["account_id", "customer_id"])

# 5. Save the cleaned data to curated bucket
curated_path = "gs://curated_data_silver/accounts"
cleaned_accounts_df.write.mode("overwrite").parquet(curated_path)


print("Data cleaning and transformation completed successfully.")
