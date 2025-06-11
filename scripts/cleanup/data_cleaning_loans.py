from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

spark = SparkSession.builder.appName("DataCleaningLoans").getOrCreate()

loans_schema = StructType([
    StructField("loan_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("loan_amount", DoubleType(), True),
    StructField("interest_rate", DoubleType(), True),
    StructField("loan_term", IntegerType(), True)
])

raw_path = "gs://raw_data_bronze/loans.csv"
loans_df = spark.read.csv(raw_path, header=True, schema=loans_schema)

cleaned_loans_df = loans_df.dropna(subset=["loan_id", "customer_id"])

curated_path = "gs://curated_data_silver/loans"
cleaned_loans_df.write.mode("append").parquet(curated_path)

print("Data cleaning for loans.csv completed.")
