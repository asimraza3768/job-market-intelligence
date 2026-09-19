# Databricks notebook source

#  %md
#  # 03 - Gold Analytics
# 
#  Creates business-ready analytics from the Silver job dataset.
# 
#  The Gold layer provides:
#  - Skill demand
#  - Country market intelligence
#  - Job/profile skill coverage
#  - Skill gaps
#  - Skill co-occurrence
#  - Skill recommendations
# 
#  **Source:** `job_market.silver.jobs`
#  **Destination:** `job_market.gold`

# COMMAND ----------

from pyspark.sql.functions import (
    array,
    array_distinct,
    array_except,
    array_intersect,
    col,
    count,
    count_distinct,
    explode,
    explode_outer,
    lit,
    round,
    size,
    split,
    sum,
    transform,
    trim,
    when
)

# COMMAND ----------

# Read the Silver job dataset.
silver_df = (
    spark.read
    .format("delta")
    .table("job_market.silver.jobs")
)

print("Silver jobs:", silver_df.count())

# COMMAND ----------

#  %md
#  ## 1. Skill Demand

# COMMAND ----------

# Convert the comma-separated technology list into
# individual technology records.

technology_df = (
    silver_df
    .withColumn(
        "technology",
        explode_outer(
            split(col("technologies"), ",")
        )
    )
)

# COMMAND ----------

# Count how many job postings require each technology.

skill_demand_df = (
    technology_df
    .filter(col("technology").isNotNull())
    .withColumn(
        "technology",
        trim(col("technology"))
    )
    .filter(col("technology") != "")
    .groupBy("technology")
    .count()
    .withColumnRenamed("count", "job_count")
    .orderBy(col("job_count").desc())
)

print("Unique skills:", skill_demand_df.count())

# COMMAND ----------

# Save skill demand analytics.
(
    skill_demand_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("job_market.gold.skill_demand")
)

# COMMAND ----------

#  %md
#  ## 2. Country Market Intelligence

# COMMAND ----------

country_market_df = (
    silver_df
    .groupBy("country")
    .agg(
        count("*").alias("job_count"),
        sum(
            when(col("remote") == True, 1)
            .otherwise(0)
        ).alias("remote_jobs"),
        sum(
            when(col("hybrid") == True, 1)
            .otherwise(0)
        ).alias("hybrid_jobs")
    )
    .orderBy(col("job_count").desc())
)

# COMMAND ----------

(
    country_market_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("job_market.gold.country_market")
)

# COMMAND ----------

#  %md
#  ## 3. Job Matching and Profile Skill Coverage
# 
#  The profile skill list represents the skills currently included
#  in the Data Engineering portfolio/profile.

# COMMAND ----------

user_skills = [
    "python",
    "sql",
    "etl",
    "data-engineering",
    "databricks",
    "apache-spark",
    "data-modeling",
    "data-warehousing",
    "azure",
    "data-governance",
    "git",
    "github",
    "pandas",
    "rest-api"
]

total_user_skills = len(user_skills)

user_skills_array = array(
    [lit(skill) for skill in user_skills]
)

# COMMAND ----------

# Convert each job's technology string into an array
# and compare it with the profile skill list.

matched_df = (
    silver_df
    .withColumn(
        "job_skill_array",
        split(col("technologies"), ",")
    )
    .withColumn(
        "job_skill_array",
        transform(
            col("job_skill_array"),
            lambda x: trim(x)
        )
    )
    .withColumn(
        "matched_skills",
        array_intersect(
            col("job_skill_array"),
            user_skills_array
        )
    )
    .withColumn(
        "missing_skills",
        array_except(
            col("job_skill_array"),
            user_skills_array
        )
    )
    .withColumn(
        "match_count",
        size(col("matched_skills"))
    )
    .withColumn(
        "total_job_skills",
        size(col("job_skill_array"))
    )
    .withColumn(
        "job_skill_coverage",
        when(
            col("total_job_skills") > 0,
            round(
                (
                    col("match_count") /
                    col("total_job_skills")
                ) * 100,
                2
            )
        ).otherwise(0)
    )
    .withColumn(
        "profile_skill_match",
        when(
            col("total_job_skills") > 0,
            round(
                (
                    col("match_count") /
                    lit(total_user_skills)
                ) * 100,
                2
            )
        ).otherwise(0)
    )
)

# COMMAND ----------

job_matches_df = matched_df.select(
    "job_id",
    "job_title",
    "company",
    "country",
    "matched_skills",
    "missing_skills",
    "match_count",
    "job_skill_coverage",
    "profile_skill_match",
    "url"
)

# COMMAND ----------

(
    job_matches_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("job_market.gold.job_matches")
)

# COMMAND ----------

#  %md
#  ## 4. Skill Gap Intelligence

# COMMAND ----------

# Find technologies required by jobs that are not
# currently included in the profile skill list.

missing_skill_per_job_df = (
    silver_df
    .withColumn(
        "job_skills",
        split(col("technologies"), ",")
    )
    .withColumn(
        "job_skills",
        transform(
            col("job_skills"),
            lambda x: trim(x)
        )
    )
    .withColumn(
        "missing_skills_list",
        array_except(
            col("job_skills"),
            user_skills_array
        )
    )
)

# COMMAND ----------

skill_gaps_df = (
    missing_skill_per_job_df
    .withColumn(
        "missing_skill",
        explode(col("missing_skills_list"))
    )
    .filter(
        col("missing_skill").isNotNull() &
        (col("missing_skill") != "")
    )
    .groupBy("missing_skill")
    .agg(
        count_distinct("job_id").alias("job_count")
    )
    .orderBy(col("job_count").desc())
)

# COMMAND ----------

(
    skill_gaps_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("job_market.gold.skill_gaps")
)

# COMMAND ----------

#  %md
#  ## 5. Skill Co-occurrence
# 
#  Identifies technologies that commonly appear together
#  in Data Engineering job postings.

# COMMAND ----------

de_jobs_df = (
    spark.read
    .format("delta")
    .table("job_market.silver.jobs")
    .filter(
        col("job_title").ilike("%Data Engineer%")
    )
    .withColumn(
        "raw_skills_array",
        split(col("technologies"), ",")
    )
    .withColumn(
        "trimmed_array",
        transform(
            col("raw_skills_array"),
            lambda x: trim(x)
        )
    )
    .withColumn(
        "clean_skills_array",
        array_distinct(col("trimmed_array"))
    )
    .select(
        "job_id",
        "clean_skills_array"
    )
)

# COMMAND ----------

# Explode the skills twice to generate skill pairs.
df_skill_1 = de_jobs_df.withColumn(
    "skill_1",
    explode("clean_skills_array")
)

df_skill_2 = de_jobs_df.withColumn(
    "skill_2",
    explode("clean_skills_array")
)

# COMMAND ----------

co_occurrence_df = (
    df_skill_1
    .join(
        df_skill_2,
        on="job_id"
    )
    .filter(
        col("skill_1") < col("skill_2")
    )
    .groupBy(
        "skill_1",
        "skill_2"
    )
    .count()
    .withColumnRenamed(
        "count",
        "job_count"
    )
    .orderBy(
        col("job_count").desc()
    )
)

final_co_occurrence_df = co_occurrence_df.select(
    "skill_1",
    "skill_2",
    "job_count"
)

# COMMAND ----------

(
    final_co_occurrence_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(
        "job_market.gold.co_occurence"
    )
)

# COMMAND ----------

#  %md
#  ## 6. Skill Recommendations
# 
#  Recommendations are based on skills that:
#  1. Are missing from the profile
#  2. Appear frequently in Data Engineer job postings
# 
#  The demand percentage represents the share of analyzed
#  Data Engineer jobs requiring each missing skill.

# COMMAND ----------

skill_demand_df = (
    spark.read
    .format("delta")
    .table("job_market.gold.skill_demand")
)

# COMMAND ----------

total_jobs = (
    silver_df
    .filter(
        col("job_title").ilike("%Data Engineer%")
    )
    .select("job_id")
    .distinct()
    .count()
)

print("Data Engineer jobs analyzed:", total_jobs)

# COMMAND ----------

recommendation_df = (
    skill_gaps_df
    .join(
        skill_demand_df,
        skill_gaps_df["missing_skill"] ==
        skill_demand_df["technology"],
        "left"
    )
    .select(
        skill_gaps_df["missing_skill"].alias("skill"),
        skill_gaps_df["job_count"],
        round(
            (
                skill_gaps_df["job_count"] /
                lit(total_jobs)
            ) * 100,
            2
        ).alias("demand_percentage")
    )
    .orderBy(
        col("job_count").desc()
    )
)

# COMMAND ----------

(
    recommendation_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(
        "job_market.gold.recommendations"
    )
)

# COMMAND ----------


 ## Gold Layer Summary

# COMMAND ----------

print("========== GOLD ANALYTICS ==========")
print("Skill demand rows:",
      skill_demand_df.count())
print("Country market rows:",
      country_market_df.count())
print("Job match rows:",
      job_matches_df.count())
print("Skill gap rows:",
      skill_gaps_df.count())
print("Skill co-occurrence rows:",
      final_co_occurrence_df.count())
print("Recommendation rows:",
      recommendation_df.count())
print("====================================")
