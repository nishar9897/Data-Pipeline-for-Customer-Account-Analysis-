### Challenges Faced

Initially, I used `.write.mode("overwrite").parquet(...)` when writing Parquet files, which inadvertently caused previous data to be lost whenever the pipeline ran. I fixed this issue by modifying the code to use `.write.mode("append").parquet(...)` instead, ensuring that new data is added without deleting the existing data.
