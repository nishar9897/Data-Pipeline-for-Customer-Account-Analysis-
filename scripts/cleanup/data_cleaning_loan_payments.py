from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

spark = SparkSession.builder.appName("DataCleaningLoanPayments").getOrCreate()

loan_payments_schema = StructType([
    StructField("payment_id", StringType(), True),
    StructField("loan_id", StringType(), True),
    StructField("payment_date", StringType(), True),
    StructField("payment_amount", DoubleType(), True)
])

raw_path = "gs://raw_data_bronze/loan_payments.csv"
loan_payments_df = spark.read.csv(raw_path, header=True, schema=loan_payments_schema)

cleaned_loan_payments_df = loan_payments_df.dropna(subset=["payment_id", "loan_id"])

curated_path = "gs://curated_data_silver/loan_payments"
cleaned_loan_payments_df.write.mode("append").parquet(curated_path)

print("Data cleaning for loan_payments.csv completed.")
