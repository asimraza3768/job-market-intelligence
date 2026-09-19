# Databricks notebook source

#  %md
#  # 00 - API Ingestion
# 
#  Ingests Data Engineer job listings from the JobsPipe API
#  and stores the complete raw API responses in Amazon S3.
# 
#  **Source:** JobsPipe API  
#  **Destination:** AWS S3 Raw Layer  
#  **Orchestration:** Databricks Workflow

# COMMAND ----------

import requests
import json
from datetime import datetime, timezone

# COMMAND ----------

# Load API credentials securely from Databricks Secrets.
api_key = dbutils.secrets.get(
    scope="job-market-secrets",
    key="jobspipe-api-key"
)

print("JobsPipe API key loaded successfully.")

# COMMAND ----------

# JobsPipe API configuration.
url = "https://api.jobspipe.dev/v1/jobs/search"

headers = {
    "Authorization": f"Bearer {api_key}",
    "Content-Type": "application/json"
}

base_payload = {
    "job_title_or": ["Data Engineer"],
    "limit": 25
}

# COMMAND ----------

# Pagination configuration.
cursor = None
seen_cursors = set()
page_number = 1
max_pages = 5

# COMMAND ----------

while page_number <= max_pages:

    print(f"\nFetching page {page_number}...")

    payload = base_payload.copy()

    if cursor:
        payload["cursor"] = cursor

    response = requests.post(
        url,
        headers=headers,
        json=payload,
        timeout=60
    )

    print("Status code:", response.status_code)

    if response.status_code != 200:
        print("API error:", response.text)
        raise Exception("JobsPipe API request failed.")

    api_response = response.json()

    jobs = api_response.get("data", [])

    print(f"Received {len(jobs)} jobs")

    # UTC ingestion timestamp.
    ingestion_time = datetime.now(timezone.utc)

    date_folder = ingestion_time.strftime("%Y-%m-%d")
    timestamp = ingestion_time.strftime("%Y-%m-%dT%H-%M-%SZ")

    # Store the complete API response in the S3 raw layer.
    s3_path = (
        f"s3://job-market-pipeline-raw/jobs/"
        f"{date_folder}/"
        f"jobs_page_{page_number}_{timestamp}.json"
    )

    dbutils.fs.put(
        s3_path,
        json.dumps(
            api_response,
            indent=4,
            ensure_ascii=False
        ),
        True
    )

    print(f"Uploaded to S3: {s3_path}")

    # Get the cursor for the next page.
    metadata = api_response.get("metadata", {})
    next_cursor = metadata.get("next_cursor")

    print("Next cursor exists:", bool(next_cursor))

    # Stop when there are no more pages.
    if not next_cursor:
        print("No more pages.")
        break

    # Protect against an API returning the same cursor repeatedly.
    if next_cursor in seen_cursors:
        print("Repeated cursor detected. Stopping pagination.")
        break

    seen_cursors.add(next_cursor)
    cursor = next_cursor
    page_number += 1

print("\nS3 ingestion complete.")
