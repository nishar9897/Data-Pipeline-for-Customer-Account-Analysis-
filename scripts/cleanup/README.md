### Challenges Faced

Initially, I used the `.write.mode("overwrite")` option when writing Parquet files, which inadvertently caused previous data to be lost whenever the pipeline ran. To address this, I added a data backup step to archive the existing data before overwriting, ensuring that historical data is preserved while still keeping the latest cleaned dataset up to date.

