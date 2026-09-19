# Databricks notebook source

#  %md
#  # 01 - Bronze Ingestion
# 
#  Reads raw job API responses from Amazon S3, extracts individual job
#  records, removes duplicate job IDs, and upserts the data into the
#  Bronze Delta table.
# 
#  **Source:** AWS S3 Raw Layer  
#  **Destination:** `job_market.bronze.jobs`  
#  **Storage Format:** Delta Lake

# COMMAND ----------

from datetime import datetime, timezone

# Read only the current ingestion date.
ingestion_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

raw_path = (
    f"s3://job-market-pipeline-raw/jobs/"
    f"{ingestion_date}/*.json"
)

print("Reading raw files from:", raw_path)

# COMMAND ----------

# Read the complete raw API responses.
raw_df = (
    spark.read
    .option("multiline", True)
    .json(raw_path)
)

print("Raw response files:", raw_df.count())
raw_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col, explode

# Each API response contains job records inside the `data` array.
# Explode the array so that each job becomes an individual row.
bronze_df = raw_df.select(
    explode(col("data")).alias("job")
)

bronze_df.printSchema()

# COMMAND ----------

# Select and flatten the fields required by the Bronze layer.
bronze_df = bronze_df.select(
    col("job.id").alias("job_id"),
    col("job.job_title").alias("job_title"),
    col("job.company").alias("company"),
    col("job.location").alias("location"),
    col("job.country").alias("country"),
    col("job.country_code").alias("country_code"),
    col("job.remote").alias("remote"),
    col("job.hybrid").alias("hybrid"),
    col("job.work_arrangement").alias("work_arrangement"),
    col("job.date_posted").alias("date_posted"),
    col("job.discovered_at").alias("discovered_at"),
    col("job.seniority").alias("seniority"),
    col("job.employment_statuses").alias("employment_statuses"),
    col("job.salary_string").alias("salary_string"),
    col("job.min_annual_salary_usd").alias("min_annual_salary_usd"),
    col("job.max_annual_salary_usd").alias("max_annual_salary_usd"),
    col("job.url").alias("url"),
    col("job.technology_slugs").alias("technology_slugs"),
    col("job.description").alias("description"),
    col("job.sources").alias("sources"),
    col("job.visa_sponsorship").alias("visa_sponsorship")
)

# COMMAND ----------

# Remove duplicate jobs within the current ingestion batch.
bronze_df = bronze_df.dropDuplicates(["job_id"])

print("Unique jobs in current batch:", bronze_df.count())

# COMMAND ----------

# Rename the business key to match the Bronze table.
bronze_df = bronze_df.withColumnRenamed(
    "job_id",
    "id"
)

# COMMAND ----------

from delta.tables import DeltaTable

bronze_table = DeltaTable.forName(
    spark,
    "job_market.bronze.jobs"
)

# Upsert records into the Bronze Delta table.
#
# Existing job IDs are updated.
# New job IDs are inserted.
(
    bronze_table.alias("target")
    .merge(
        bronze_df.alias("source"),
        "target.id = source.id"
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

# COMMAND ----------

# Validate the Bronze table after the merge.
bronze_check = spark.table(
    "job_market.bronze.jobs"
)

print(
    "Total Bronze rows:",
    bronze_check.count()
)

# COMMAND ----------

# Check for duplicate IDs after the merge.
from pyspark.sql.functions import count

duplicates = (
    bronze_check
    .groupBy("id")
    .agg(count("*").alias("count"))
    .filter(col("count") > 1)
)

duplicate_count = duplicates.count()

print("Duplicate IDs:", duplicate_count)

# COMMAND ----------

# Final uniqueness check.
total_rows = bronze_check.count()
unique_ids = bronze_check.select("id").distinct().count()

print("Total rows:", total_rows)
print("Unique IDs:", unique_ids)
print("Duplicates:", total_rows - unique_ids)
