# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 00. Extract API Data to Raw Tables (Bronze Ingestion)
# MAGIC **Execution Environment**: 100% Databricks (Cloud Cluster)  
# MAGIC **Inputs**: Google Search Console API & Bing Webmaster Tools API  
# MAGIC **Secrets**: Databricks Secrets (`dbutils.secrets.get`)  
# MAGIC **Outputs**: Delta tables `harmony_catalog.seo_analytics.raw_google_search`, `harmony_catalog.seo_analytics.raw_bing_search` (Queries), and `harmony_catalog.seo_analytics.raw_bing_page_search` (Pages)  
# MAGIC **Schedule**: Suitable for daily automated Databricks Workflows (Jobs)

# COMMAND ----------

# MAGIC %pip install google-api-python-client google-auth requests pyyaml --quiet

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Initialize Catalog & Schema Context

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS harmony_catalog;
# MAGIC USE CATALOG harmony_catalog;
# MAGIC CREATE SCHEMA IF NOT EXISTS seo_analytics;
# MAGIC USE SCHEMA seo_analytics;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Configuration & Secrets Retrieval
# MAGIC Retrieves API credentials securely from Databricks Secrets with graceful fallback.

# COMMAND ----------

import json
import os
from datetime import date, timedelta
from pyspark.sql.functions import current_timestamp
from delta.tables import DeltaTable
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
# MAGIC ### 3. Google Search Console: Extract 100% Traffic to `raw_google_search`
# MAGIC Pulls revealed keywords and page-level totals, then adds rows with `query = NULL` for anonymized privacy queries.
# MAGIC Result: A single raw table capturing 100% of website traffic with zero missing clicks.

# COMMAND ----------

SCOPES = ["https://www.googleapis.com/auth/webmasters.readonly"]
creds = service_account.Credentials.from_service_account_info(gsc_service_account_info, scopes=SCOPES)
service = build("searchconsole", "v1", credentials=creds)

# 1. Fetch revealed queries
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
revealed_totals = {}

for r in rows:
    keys = r.get("keys", ["", "", ""])
    dt, qry, pg = keys[0], keys[1], keys[2]
    clk = int(r.get("clicks", 0))
    imp = int(r.get("impressions", 0))
    ctr = float(r.get("ctr", 0.0))
    pos = float(r.get("position", 0.0))

    gsc_records.append({
        "date": dt,
        "query": qry,
        "page": pg,
        "clicks": clk,
        "impressions": imp,
        "ctr": ctr,
        "position": pos
    })

    if (dt, pg) not in revealed_totals:
        revealed_totals[(dt, pg)] = {"clicks": 0, "impressions": 0}
    revealed_totals[(dt, pg)]["clicks"] += clk
    revealed_totals[(dt, pg)]["impressions"] += imp

print(f"Extracted {len(gsc_records)} revealed keyword rows.")

# 2. Fetch 100% page totals (no privacy omission)
request_body_pages = {
    "startDate": start_date,
    "endDate": end_date,
    "dimensions": ["date", "page"],
    "rowLimit": config.get("pipeline", {}).get("row_batch_limit", 25000),
    "startRow": 0
}

response_pages = service.searchanalytics().query(siteUrl=gsc_property, body=request_body_pages).execute()
rows_pages = response_pages.get("rows", [])

# 3. Add rows with query = None (NULL in SQL) for anonymized traffic
anonymized_count = 0
for r in rows_pages:
    keys = r.get("keys", ["", ""])
    dt, pg = keys[0], keys[1]
    total_clk = int(r.get("clicks", 0))
    total_imp = int(r.get("impressions", 0))

    rev = revealed_totals.get((dt, pg), {"clicks": 0, "impressions": 0})
    diff_clicks = max(0, total_clk - rev["clicks"])
    diff_impressions = max(0, total_imp - rev["impressions"])

    if diff_clicks > 0 or diff_impressions > 0:
        gsc_records.append({
            "date": dt,
            "query": None,  # Explicit NULL in raw table
            "page": pg,
            "clicks": diff_clicks,
            "impressions": diff_impressions,
            "ctr": float(diff_clicks / diff_impressions) if diff_impressions > 0 else 0.0,
            "position": float(r.get("position", 0.0))
        })
        anonymized_count += 1

print(f"Added {anonymized_count} rows with NULL query for anonymized traffic.")
print(f"Total rows in raw_google_search: {len(gsc_records)} (100% of website traffic).")

if gsc_records:
    df_google = spark.createDataFrame(gsc_records)
    df_google = df_google.withColumn("ingestion_timestamp", current_timestamp())
    df_google = df_google.dropDuplicates(["date", "page", "query"])
    
    raw_google_table = "harmony_catalog.seo_analytics.raw_google_search"
    if not spark.catalog.tableExists(raw_google_table):
        df_google.write.format("delta").saveAsTable(raw_google_table)
    else:
        delta_google = DeltaTable.forName(spark, raw_google_table)
        delta_google.alias("tgt").merge(
            df_google.alias("src"),
            """
            tgt.date = src.date AND 
            tgt.page = src.page AND 
            (tgt.query = src.query OR (tgt.query IS NULL AND src.query IS NULL))
            """
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    print("Merged 100% complete dataset into harmony_catalog.seo_analytics.raw_google_search")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Bing Webmaster Tools: Extract & Save to `raw_bing_search` (Queries) & `raw_bing_page_search` (Pages)

# COMMAND ----------

from datetime import datetime, timezone
import re

def parse_bing_date(raw_date):
    if "/Date(" in raw_date:
        match = re.search(r"\d+", raw_date)
        return datetime.fromtimestamp(int(match.group(0)) / 1000.0, timezone.utc).strftime("%Y-%m-%d") if match else raw_date
    return raw_date

# 1. Fetch Bing Query Statistics (Keyword Level)
bing_query_endpoint = "https://ssl.bing.com/webmaster/api.svc/json/GetQueryStats"
bing_params = {"siteUrl": bing_site_url, "apikey": bing_api_key}

bing_query_res = requests.get(bing_query_endpoint, params=bing_params, timeout=30)
bing_query_res.raise_for_status()
bing_query_items = bing_query_res.json().get("d", [])

bing_records = []
for item in bing_query_items:
    clean_date = parse_bing_date(item.get("Date", ""))
    clicks = int(item.get("Clicks", 0))
    impressions = int(item.get("Impressions", 0))
    avg_pos = float(item.get("AvgPosition", item.get("AvgImpressionPosition", 0.0)) or 0.0)

    bing_records.append({
        "date": clean_date,
        "query": item.get("Query", ""),
        "clicks": clicks,
        "impressions": impressions,
        "avg_position": avg_pos,

    })

print(f"Extracted {len(bing_records)} query rows from Bing Webmaster Tools.")

if bing_records:
    df_bing = spark.createDataFrame(bing_records)
    df_bing = df_bing.withColumn("ingestion_timestamp", current_timestamp())
    df_bing = df_bing.dropDuplicates(["date", "query"])
    
    raw_bing_table = "harmony_catalog.seo_analytics.raw_bing_search"
    if not spark.catalog.tableExists(raw_bing_table):
        df_bing.write.format("delta").saveAsTable(raw_bing_table)
    else:
        delta_bing = DeltaTable.forName(spark, raw_bing_table)
        delta_bing.alias("tgt").merge(
            df_bing.alias("src"),
            "tgt.date = src.date AND tgt.query = src.query"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    print("Merged into harmony_catalog.seo_analytics.raw_bing_search")

# 2. Fetch Bing Page Statistics (URL Level)
bing_page_endpoint = "https://ssl.bing.com/webmaster/api.svc/json/GetPageStats"
bing_page_res = requests.get(bing_page_endpoint, params=bing_params, timeout=30)
bing_page_res.raise_for_status()
bing_page_items = bing_page_res.json().get("d", [])

bing_page_records = []
for item in bing_page_items:
    clean_date = parse_bing_date(item.get("Date", ""))
    page_url = item.get("Query") or item.get("Page") or ""
    clicks = int(item.get("Clicks", 0))
    impressions = int(item.get("Impressions", 0))
   
    avg_pos = float(item.get("AvgImpressionPosition", item.get("AvgPosition", 0.0)) or 0.0)

    bing_page_records.append({
        "date": clean_date,
        "page": page_url,
        "clicks": clicks,
        "impressions": impressions,
        "avg_position": avg_pos
    })

print(f"Extracted {len(bing_page_records)} page rows from Bing Webmaster Tools.")

if bing_page_records:
    df_bing_page = spark.createDataFrame(bing_page_records)
    df_bing_page = df_bing_page.withColumn("ingestion_timestamp", current_timestamp())
    df_bing_page = df_bing_page.dropDuplicates(["date", "page"])
    
    raw_bing_page_table = "harmony_catalog.seo_analytics.raw_bing_page_search"
    if not spark.catalog.tableExists(raw_bing_page_table):
        df_bing_page.write.format("delta").saveAsTable(raw_bing_page_table)
    else:
        delta_bing_page = DeltaTable.forName(spark, raw_bing_page_table)
        delta_bing_page.alias("tgt").merge(
            df_bing_page.alias("src"),
            "tgt.date = src.date AND tgt.page = src.page"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    print("Merged into harmony_catalog.seo_analytics.raw_bing_page_search")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Verify Raw Tables Ingestion

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     'Google (Revealed Keywords)' AS metric, 
# MAGIC     count(*) AS total_rows, 
# MAGIC     sum(clicks) AS total_clicks,
# MAGIC     sum(impressions) AS total_impressions 
# MAGIC FROM harmony_catalog.seo_analytics.raw_google_search
# MAGIC WHERE query IS NOT NULL
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC     'Google (Anonymized / Null Query)' AS metric, 
# MAGIC     count(*) AS total_rows, 
# MAGIC     sum(clicks) AS total_clicks,
# MAGIC     sum(impressions) AS total_impressions 
# MAGIC FROM harmony_catalog.seo_analytics.raw_google_search
# MAGIC WHERE query IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC     'Google Total (100% Traffic)' AS metric, 
# MAGIC     count(*) AS total_rows, 
# MAGIC     sum(clicks) AS total_clicks,
# MAGIC     sum(impressions) AS total_impressions 
# MAGIC FROM harmony_catalog.seo_analytics.raw_google_search
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC     'Bing (Queries)' AS metric, 
# MAGIC     count(*) AS total_rows, 
# MAGIC     sum(clicks) AS total_clicks,
# MAGIC     sum(impressions) AS total_impressions 
# MAGIC FROM harmony_catalog.seo_analytics.raw_bing_search
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC     'Bing (Pages)' AS metric, 
# MAGIC     count(*) AS total_rows, 
# MAGIC     sum(clicks) AS total_clicks,
# MAGIC     sum(impressions) AS total_impressions
# MAGIC FROM harmony_catalog.seo_analytics.raw_bing_page_search;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from harmony_catalog.seo_analytics.raw_bing_page_search;
# MAGIC select * from  harmony_catalog.seo_analytics.raw_bing_search;
# MAGIC