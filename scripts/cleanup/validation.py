from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("VerifyOutput").getOrCreate()

# Read the output Parquet file
output_df = spark.read.parquet("gs://curated_data_silver/accounts/")


# Show some sample rows
output_df.show(5)
