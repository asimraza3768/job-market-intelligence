# Databricks notebook source

#  %md
#  # 02 - Silver Transformation
# 
#  Transforms the Bronze job data into a clean, analysis-ready Silver
#  dataset and performs data quality validation before downstream analytics.
# 
#  **Source:** `job_market.bronze.jobs`  
#  **Destination:** `job_market.silver.jobs`  
#  **Storage Format:** Delta Lake

# COMMAND ----------

from pyspark.sql.functions import (
    col,
    concat_ws,
    get,
    length,
    to_timestamp
)

# COMMAND ----------

# Read the Bronze Delta table.
silver_df = (
    spark.read
    .format("delta")
    .table("job_market.bronze.jobs")
)

print("Bronze rows:", silver_df.count())

# COMMAND ----------

# Rename the Bronze `id` field to `job_id` and select
# the fields required by the Silver layer.
silver_df = silver_df.select(
    col("id").alias("job_id"),
    col("job_title"),
    col("company"),
    col("location"),
    col("country"),
    col("country_code"),
    col("remote"),
    col("hybrid"),
    col("work_arrangement"),
    col("date_posted"),
    col("discovered_at"),
    col("seniority"),
    col("employment_statuses"),
    col("salary_string"),
    col("min_annual_salary_usd"),
    col("max_annual_salary_usd"),
    col("url"),
    col("technology_slugs"),
    col("description"),
    col("sources"),
    col("visa_sponsorship")
)

# COMMAND ----------

# Remove duplicate jobs within the transformation batch.
silver_df = silver_df.dropDuplicates(["job_id"])

print("Unique Silver rows:", silver_df.count())

# COMMAND ----------

# Flatten nested API fields into analysis-friendly columns.

silver_df = silver_df.withColumn(
    "employment_status",
    get(col("employment_statuses"), 0)
)

silver_df = silver_df.withColumn(
    "technologies",
    concat_ws(",", col("technology_slugs"))
)

silver_df = silver_df.withColumn(
    "source",
    get(col("sources"), 0)["provider"]
)

# COMMAND ----------

# Standardize salary fields as numeric values.

silver_df = silver_df.withColumn(
    "min_salary_usd",
    col("min_annual_salary_usd").cast("double")
)

silver_df = silver_df.withColumn(
    "max_salary_usd",
    col("max_annual_salary_usd").cast("double")
)

# COMMAND ----------

# Convert API timestamp strings into Spark timestamp types.

silver_df = silver_df.withColumn(
    "date_posted",
    to_timestamp(col("date_posted"))
)

silver_df = silver_df.withColumn(
    "discovered_at",
    to_timestamp(col("discovered_at"))
)

# Remove nested/source columns that are no longer required
# after their useful fields have been extracted.

silver_df = silver_df.drop(
    "employment_statuses",
    "sources",
    "min_annual_salary_usd",
    "max_annual_salary_usd",
    "technology_slugs"
)

silver_df.printSchema()

# COMMAND ----------

#  %md
#  ## Data Quality Validation

# COMMAND ----------

# Required field validation.
required_fields_check = silver_df.filter(
    col("job_id").isNotNull() &
    col("job_title").isNotNull() &
    col("company").isNotNull()
)

required_fields_pass = (
    required_fields_check.count() == silver_df.count()
)

# COMMAND ----------

# Duplicate job ID validation.
duplicate_job_id_check = (
    silver_df
    .groupBy("job_id")
    .count()
    .filter(col("count") > 1)
)

duplicate_count = duplicate_job_id_check.count()
duplicates_pass = duplicate_count == 0

# COMMAND ----------

# Salary validation.

negative_min_salary = silver_df.filter(
    col("min_salary_usd") < 0
).count()

negative_max_salary = silver_df.filter(
    col("max_salary_usd") < 0
).count()

invalid_salary_range = silver_df.filter(
    (col("min_salary_usd").isNotNull()) &
    (col("max_salary_usd").isNotNull()) &
    (col("min_salary_usd") > col("max_salary_usd"))
).count()

negative_salary_pass = (
    negative_min_salary == 0 and
    negative_max_salary == 0
)

salary_range_pass = invalid_salary_range == 0

# COMMAND ----------

# Timestamp validation.

invalid_date_posted = silver_df.filter(
    col("date_posted").isNull()
).count()

invalid_discovered_at = silver_df.filter(
    col("discovered_at").isNull()
).count()

timestamp_parse_pass = (
    invalid_date_posted == 0 and
    invalid_discovered_at == 0
)

# Some records may contain discovery timestamps earlier than
# their posting timestamps. These are flagged as warnings
# instead of being rejected.

timestamp_order_issues = silver_df.filter(
    (col("date_posted").isNotNull()) &
    (col("discovered_at").isNotNull()) &
    (col("discovered_at") < col("date_posted"))
).count()

# COMMAND ----------

# Remote field validation.
invalid_remote_values = silver_df.filter(
    col("remote").isNotNull() &
    ~col("remote").isin(True, False)
).count()

remote_values_pass = invalid_remote_values == 0

# COMMAND ----------

# Country code validation.
invalid_country_codes = silver_df.filter(
    (col("country_code").isNotNull()) &
    (length(col("country_code")) != 2)
).count()

country_codes_pass = invalid_country_codes == 0

# COMMAND ----------

# Review categorical values returned by the API.
print("Work arrangement values:")
silver_df.select("work_arrangement").distinct().show()

print("Seniority values:")
silver_df.select("seniority").distinct().show()

# COMMAND ----------

#  %md
#  ## Data Quality Summary

# COMMAND ----------

timestamp_order_warning = timestamp_order_issues > 0

print("========== SILVER DATA QUALITY ==========")
print(
    "Required fields:       ",
    "PASS" if required_fields_pass else "FAIL"
)
print(
    "Duplicate job IDs:     ",
    "PASS" if duplicates_pass else "FAIL"
)
print(
    "Negative salaries:     ",
    "PASS" if negative_salary_pass else "FAIL"
)
print(
    "Salary ranges:         ",
    "PASS" if salary_range_pass else "FAIL"
)
print(
    "Timestamp parsing:     ",
    "PASS" if timestamp_parse_pass else "FAIL"
)
print(
    "Remote values:         ",
    "PASS" if remote_values_pass else "FAIL"
)
print(
    "Country codes:         ",
    "PASS" if country_codes_pass else "FAIL"
)
print(
    "Timestamp ordering:    ",
    "WARNING" if timestamp_order_warning else "PASS"
)
print("==========================================")

# COMMAND ----------

#  %md
#  ## Merge Into Silver Delta Table

# COMMAND ----------

from delta.tables import DeltaTable

silver_table = DeltaTable.forName(
    spark,
    "job_market.silver.jobs"
)

(
    silver_table.alias("target")
    .merge(
        silver_df.alias("source"),
        "target.job_id = source.job_id"
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

# COMMAND ----------

#  %md
#  ## Post-Merge Validation

# COMMAND ----------

silver_check = spark.table(
    "job_market.silver.jobs"
)

total_rows = silver_check.count()
unique_ids = silver_check.select("job_id").distinct().count()

print("Silver rows:", total_rows)
print("Unique IDs:", unique_ids)
print("Duplicates:", total_rows - unique_ids)

# COMMAND ----------

# Final required-field check against the persisted Silver table.
final_required_fields = silver_check.filter(
    col("job_id").isNull() |
    col("job_title").isNull() |
    col("company").isNull()
).count()

print(
    "Final required-field violations:",
    final_required_fields
)
