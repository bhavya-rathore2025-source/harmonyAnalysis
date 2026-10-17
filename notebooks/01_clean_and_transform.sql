-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Databricks Silver Layer Transformations (Pure SQL)
-- MAGIC **Engine**: Spark SQL / Databricks SQL
-- MAGIC **Inputs**: Raw JSON extracts (`data/raw/*.json` or DBFS/Volume paths)
-- MAGIC **Outputs**: Delta tables `silver_google_search` and `silver_bing_search`

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ### 1. Setup Database / Schema
CREATE SCHEMA IF NOT EXISTS main.seo_analytics;
USE main.seo_analytics;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ### 1. Inspect Raw Data Tables
-- MAGIC Once you upload the JSON files via 'Add or upload data' into raw_google_search and raw_bing_search:

SELECT * FROM raw_google_search LIMIT 10;

-- COMMAND ----------
SELECT * FROM raw_bing_search LIMIT 10;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ### 2. Transform & Create Google Search Silver Table (Pure SQL)

CREATE OR REPLACE TABLE silver_google_search AS
SELECT
    TO_DATE(date, 'yyyy-MM-dd') AS date,
    LOWER(TRIM(query)) AS search_query,
    
    -- URL Normalization: strip protocol, trailing slashes, and query parameters
    LOWER(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(page, '^https?://', ''), '\\?.*$', ''), '/+$', '')) AS landing_page,
    
    -- Dynamic Category Extraction: pulls first path segment (/4-7-8-breathing/ -> 4-7-8-breathing)
    CASE 
        WHEN REGEXP_EXTRACT(page, '^https?://[^/]+/([^/?#]+)', 1) = '' THEN 'home'
        ELSE LOWER(REGEXP_EXTRACT(page, '^https?://[^/]+/([^/?#]+)', 1))
    END AS page_category,
    
    -- Brand vs. Non-Brand Classification
    CASE 
        WHEN LOWER(query) LIKE '%harmony%' THEN 'Brand'
        ELSE 'Non-Brand'
    END AS brand_type,
    
    -- Short-tail vs. Long-tail Classification
    CASE 
        WHEN SIZE(SPLIT(TRIM(query), ' ')) <= 2 THEN 'Short-Tail'
        ELSE 'Long-Tail'
    END AS query_length_type,
    
    CAST(clicks AS INT) AS clicks,
    CAST(impressions AS INT) AS impressions,
    CAST(ctr AS DOUBLE) AS ctr,
    CAST(position AS DOUBLE) AS avg_position
FROM raw_google_search
WHERE query IS NOT NULL 
  AND impressions >= clicks;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ### 3. Transform & Create Bing Webmaster Silver Table (Pure SQL)

CREATE OR REPLACE TABLE silver_bing_search AS
SELECT
    TO_DATE(date) AS date,
    LOWER(TRIM(query)) AS search_query,
    
    -- Brand vs. Non-Brand Classification
    CASE 
        WHEN LOWER(query) LIKE '%harmony%' THEN 'Brand'
        ELSE 'Non-Brand'
    END AS brand_type,
    
    -- Short-tail vs. Long-tail Classification
    CASE 
        WHEN SIZE(SPLIT(TRIM(query), ' ')) <= 2 THEN 'Short-Tail'
        ELSE 'Long-Tail'
    END AS query_length_type,
    
    CAST(clicks AS INT) AS clicks,
    CAST(impressions AS INT) AS impressions,
    CAST(ctr AS DOUBLE) AS ctr,
    CAST(avg_position AS DOUBLE) AS avg_position
FROM raw_bing_search
WHERE query IS NOT NULL;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ### 4. Verify Silver Tables
SELECT 'Google' AS source, COUNT(*) AS row_count, SUM(clicks) AS total_clicks FROM silver_google_search
UNION ALL
SELECT 'Bing' AS source, COUNT(*) AS row_count, SUM(clicks) AS total_clicks FROM silver_bing_search;
