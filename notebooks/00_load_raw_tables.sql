-- ============================================================
-- 00. Raw Data Layer Ingestion (Bronze / Raw Tables)
-- Engine: Databricks SQL / Spark SQL
-- Inputs: Raw JSON extracts (*_gsc_raw.json, *_bing_raw.json)
-- Outputs: Raw Delta tables (raw_google_search, raw_bing_search)
-- ============================================================

-- 1. Setup Catalog and Schema
-- Note: In Unity Catalog workspaces, creates 'harmony_catalog'.
-- In Community Edition (without Unity Catalog), you can use default/hive_metastore.
CREATE CATALOG IF NOT EXISTS harmony_catalog;
USE CATALOG harmony_catalog;

CREATE SCHEMA IF NOT EXISTS seo_analytics;
USE SCHEMA seo_analytics;

-- 2. Ingest Raw Google Search Console Data
-- Reads the raw JSON and creates raw_google_search Delta table
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
FROM json.`/Volumes/harmony_catalog/seo_analytics/raw_files/*_gsc_raw.json`;

-- 3. Ingest Raw Bing Webmaster Data
-- Reads the raw JSON and creates raw_bing_search Delta table
CREATE OR REPLACE TABLE raw_bing_search AS
SELECT 
    date,
    query,
    CAST(clicks AS INT) AS clicks,
    CAST(impressions AS INT) AS impressions,
    CAST(avg_position AS DOUBLE) AS avg_position,
    CAST(avg_ctr AS DOUBLE) AS avg_ctr,
    current_timestamp() AS ingestion_timestamp
FROM json.`/Volumes/harmony_catalog/seo_analytics/raw_files/*_bing_raw.json`;

-- 4. Verify & Inspect Raw Tables
SELECT 'Raw Google' AS table_name, count(*) AS total_rows FROM raw_google_search
UNION ALL
SELECT 'Raw Bing' AS table_name, count(*) AS total_rows FROM raw_bing_search;

SELECT * FROM raw_google_search LIMIT 10;
SELECT * FROM raw_bing_search LIMIT 10;
