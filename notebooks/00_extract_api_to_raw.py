# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # 00. Extract API Data to Raw Tables (Bronze Ingestion)
# MAGIC **Execution Environment**: 100% Databricks (Cloud Cluster)  
# MAGIC **Inputs**: Google Search Console API & Bing Webmaster Tools API  
# MAGIC **Secrets**: Databricks Secrets (`dbutils.secrets.get`)  
# MAGIC **Outputs**: Delta tables `harmony_catalog.seo_analytics.raw_google_search` and `harmony_catalog.seo_analytics.raw_bing_search`  
# MAGIC **Schedule**: Suitable for daily automated Databricks Workflows (Jobs)

# COMMAND ----------
# MAGIC %pip install google-api-python-client google-auth requests pyyaml --quiet

# COMMAND ----------
# MAGIC %md
# MAGIC ### 1. Initialize Catalog & Schema Context

# COMMAND ----------
# Create catalog and schema using Spark SQL
spark.sql("CREATE CATALOG IF NOT EXISTS harmony_catalog")
spark.sql("USE CATALOG harmony_catalog")
spark.sql("CREATE SCHEMA IF NOT EXISTS seo_analytics")
spark.sql("USE SCHEMA seo_analytics")

# COMMAND ----------
# MAGIC %md
# MAGIC ### 2. Configuration & Secrets Retrieval
# MAGIC Retrieves API credentials securely from Databricks Secrets with graceful fallback.

# COMMAND ----------
import json
import os
from datetime import date, timedelta
from pyspark.sql.functions import current_timestamp
from google.oauth2 import service_account
from googleapiclient.discovery import build
import requests

# Load config settings from repo
with open("../config/config.yaml", "r", encoding="utf-8") as f:
    import yaml
    config = yaml.safe_load(f)

site_name = config["website"]["name"]
gsc_property = config["website"]["gsc_property_uri"]
bing_site_url = config["website"]["bing_site_url"]
lookback_days = config.get("pipeline", {}).get("lookback_days", 30)

# Retrieve credentials: Check workspace file first, then Secrets, then Environment Variables
SECRET_SCOPE = "seo_secrets"

# 1. Bing API Key
bing_api_key = os.environ.get("BING_API_KEY", "")
if not bing_api_key:
    try:
        bing_api_key = dbutils.secrets.get(scope=SECRET_SCOPE, key="bing_api_key")
    except Exception:
        pass

# Fallback: check .env in workspace root
if not bing_api_key and os.path.exists("../.env"):
    with open("../.env", "r", encoding="utf-8") as ef:
        for line in ef:
            if line.strip().startswith("BING_API_KEY="):
                bing_api_key = line.strip().split("=", 1)[1].strip()

if not bing_api_key:
    raise ValueError("Bing API Key not found. Please add BING_API_KEY to .env, Databricks Secrets, or Cluster Environment variables.")

# 2. Google Service Account Credentials
gsc_service_account_info = None
creds_file_path = f"../{config['credentials']['gsc_service_account_json']}"

# Check if file exists in Databricks workspace credentials/ folder
if os.path.exists(creds_file_path):
    with open(creds_file_path, "r", encoding="utf-8") as f:
        gsc_service_account_info = json.load(f)
    print(f"Loaded Google credentials from workspace file: {creds_file_path}")
else:
    # Check Databricks Secrets
    try:
        gsc_secret_str = dbutils.secrets.get(scope=SECRET_SCOPE, key="gsc_service_account_json")
        gsc_service_account_info = json.loads(gsc_secret_str)
        print("Loaded Google credentials from Databricks Secrets.")
    except Exception:
        # Check Environment Variables
        gsc_env_str = os.environ.get("GSC_SERVICE_ACCOUNT_JSON", "")
        if gsc_env_str:
            gsc_service_account_info = json.loads(gsc_env_str)
            print("Loaded Google credentials from Cluster Environment Variables.")

if not gsc_service_account_info:
    raise FileNotFoundError(
        f"Google Service Account key not found at '{creds_file_path}'. "
        "Create a 'credentials/' folder in Databricks and upload your JSON key, or use Databricks Secrets."
    )

# Calculate dynamic dates (with 2-day GSC lag)
end_date = (date.today() - timedelta(days=2)).strftime("%Y-%m-%d")
start_date = (date.today() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")

print(f"Target Site: {site_name}")
print(f"Extraction Window: {start_date} to {end_date}")

# COMMAND ----------
# MAGIC %md
# MAGIC ### 3. Google Search Console: Extract & Save to `raw_google_search`

# COMMAND ----------
SCOPES = ["https://www.googleapis.com/auth/webmasters.readonly"]
creds = service_account.Credentials.from_service_account_info(gsc_service_account_info, scopes=SCOPES)
service = build("searchconsole", "v1", credentials=creds)

request_body = {
    "startDate": start_date,
    "endDate": end_date,
    "dimensions": ["date", "query", "page"],
    "rowLimit": config.get("pipeline", {}).get("row_batch_limit", 25000),
    "startRow": 0
}

response = service.searchanalytics().query(siteUrl=gsc_property, body=request_body).execute()
rows = response.get("rows", [])

gsc_records = []
for r in rows:
    keys = r.get("keys", ["", "", ""])
    gsc_records.append({
        "date": keys[0],
        "query": keys[1],
        "page": keys[2],
        "clicks": int(r.get("clicks", 0)),
        "impressions": int(r.get("impressions", 0)),
        "ctr": float(r.get("ctr", 0.0)),
        "position": float(r.get("position", 0.0))
    })

print(f"Extracted {len(gsc_records)} rows from Google Search Console.")

if gsc_records:
    df_google = spark.createDataFrame(gsc_records)
    df_google = df_google.withColumn("ingestion_timestamp", current_timestamp())
    df_google.write.format("delta").mode("overwrite").saveAsTable("harmony_catalog.seo_analytics.raw_google_search")
    print("Saved keyword stream to harmony_catalog.seo_analytics.raw_google_search")

# COMMAND ----------
# MAGIC %md
# MAGIC ### 3b. Google Search Console: Stream 2 (100% Total Page Traffic - Zero Privacy Filter)
# MAGIC Uses dimensions `["date", "page"]` without `query`. Google includes 100% of clicks and impressions (including anonymized long-tail searches).

# COMMAND ----------
request_body_pages = {
    "startDate": start_date,
    "endDate": end_date,
    "dimensions": ["date", "page"],
    "rowLimit": config.get("pipeline", {}).get("row_batch_limit", 25000),
    "startRow": 0
}

response_pages = service.searchanalytics().query(siteUrl=gsc_property, body=request_body_pages).execute()
rows_pages = response_pages.get("rows", [])

gsc_page_records = []
for r in rows_pages:
    keys = r.get("keys", ["", ""])
    gsc_page_records.append({
        "date": keys[0],
        "page": keys[1],
        "clicks": int(r.get("clicks", 0)),
        "impressions": int(r.get("impressions", 0)),
        "ctr": float(r.get("ctr", 0.0)),
        "position": float(r.get("position", 0.0))
    })

print(f"Extracted {len(gsc_page_records)} page-level rows (100% traffic, zero privacy omissions).")

if gsc_page_records:
    df_google_pages = spark.createDataFrame(gsc_page_records)
    df_google_pages = df_google_pages.withColumn("ingestion_timestamp", current_timestamp())
    df_google_pages.write.format("delta").mode("overwrite").saveAsTable("harmony_catalog.seo_analytics.raw_google_page_totals")
    print("Saved page totals to harmony_catalog.seo_analytics.raw_google_page_totals")

# COMMAND ----------
# MAGIC %md
# MAGIC ### 4. Bing Webmaster Tools: Extract & Save to `raw_bing_search`

# COMMAND ----------
from datetime import datetime, timezone
import re

bing_endpoint = "https://ssl.bing.com/webmaster/api.svc/json/GetQueryStats"
bing_params = {"siteUrl": bing_site_url, "apikey": bing_api_key}

bing_res = requests.get(bing_endpoint, params=bing_params, timeout=30)
bing_res.raise_for_status()
bing_items = bing_res.json().get("d", [])

bing_records = []
for item in bing_items:
    raw_date = item.get("Date", "")
    if "/Date(" in raw_date:
        match = re.search(r"\d+", raw_date)
        clean_date = datetime.fromtimestamp(int(match.group(0)) / 1000.0, timezone.utc).strftime("%Y-%m-%d") if match else raw_date
    else:
        clean_date = raw_date

    bing_records.append({
        "date": clean_date,
        "query": item.get("Query", ""),
        "clicks": int(item.get("Clicks", 0)),
        "impressions": int(item.get("Impressions", 0)),
        "avg_position": float(item.get("AvgPosition", 0.0)),
        "avg_ctr": float(item.get("Ctr", 0.0))
    })

print(f"Extracted {len(bing_records)} rows from Bing Webmaster Tools.")

if bing_records:
    df_bing = spark.createDataFrame(bing_records)
    df_bing = df_bing.withColumn("ingestion_timestamp", current_timestamp())
    df_bing.write.format("delta").mode("overwrite").saveAsTable("harmony_catalog.seo_analytics.raw_bing_search")
    print("Saved to harmony_catalog.seo_analytics.raw_bing_search")

# COMMAND ----------
# MAGIC %md
# MAGIC ### 5. Verify Raw Tables Ingestion

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT 'Raw Google (Keywords)' AS table_name, count(*) AS total_rows, sum(clicks) AS total_clicks FROM harmony_catalog.seo_analytics.raw_google_search
# MAGIC UNION ALL
# MAGIC SELECT 'Raw Google (Page Totals - 100%)' AS table_name, count(*) AS total_rows, sum(clicks) AS total_clicks FROM harmony_catalog.seo_analytics.raw_google_page_totals
# MAGIC UNION ALL
# MAGIC SELECT 'Raw Bing' AS table_name, count(*) AS total_rows, sum(clicks) AS total_clicks FROM harmony_catalog.seo_analytics.raw_bing_search;
