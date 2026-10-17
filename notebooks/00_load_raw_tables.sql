-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 00. Raw Data Layer Ingestion (Bronze / Raw Tables)
-- MAGIC **Step**: 1 of 2 in Databricks Pipeline
-- MAGIC **Inputs**: Raw JSON extracts (`*_gsc_raw.json`, `*_bing_raw.json`)
-- MAGIC **Outputs**: Raw Delta tables `raw_google_search` and `raw_bing_search`
-- MAGIC **Purpose**: Preserve exact raw data from APIs without any transformations.

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ### 1. Initialize Database Schema
CREATE SCHEMA IF NOT EXISTS main.seo_analytics;
USE main.seo_analytics;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ### 2. Ingest Raw Google Search Console Data
-- MAGIC Reads the raw JSON and creates `raw_google_search` Delta table

-- Adjust path based on your Databricks storage (Volumes, DBFS, or workspace folder)
-- e.g. /Volumes/main/seo_analytics/raw_files/harmonybreath_gsc_raw.json
-- or /FileStore/tables/harmonybreath_gsc_raw.json

CREATE OR REPLACE TABLE raw_google_search AS
SELECT 
    date,
    query,
    page,
    CAST(clicks AS INT) AS clicks,
    CAST(impressions AS INT) AS impressions,
    CAST(ctr AS DOUBLE) AS ctr,
    CAST(position AS DOUBLE) AS position,
    current_timestamp() AS ingestion_timestamp
FROM json.`/Volumes/main/seo_analytics/raw_files/*_gsc_raw.json`;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ### 3. Ingest Raw Bing Webmaster Data
-- MAGIC Reads the raw JSON and creates `raw_bing_search` Delta table

CREATE OR REPLACE TABLE raw_bing_search AS
SELECT 
    date,
    query,
    CAST(clicks AS INT) AS clicks,
    CAST(impressions AS INT) AS impressions,
    CAST(avg_position AS DOUBLE) AS avg_position,
    CAST(avg_ctr AS DOUBLE) AS avg_ctr,
    current_timestamp() AS ingestion_timestamp
FROM json.`/Volumes/main/seo_analytics/raw_files/*_bing_raw.json`;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ### 4. Verify & Inspect Raw Tables

SELECT 'Raw Google' AS table_name, count(*) AS total_rows FROM raw_google_search
UNION ALL
SELECT 'Raw Bing' AS table_name, count(*) AS total_rows FROM raw_bing_search;

-- COMMAND ----------
SELECT * FROM raw_google_search LIMIT 10;

-- COMMAND ----------
SELECT * FROM raw_bing_search LIMIT 10;
