-- Databricks notebook source
-- COMMAND ----------
-- MAGIC %md
-- MAGIC # 01. Clean & Transform (Silver Layer)
-- MAGIC **Inputs**: `raw_google_search`, `raw_bing_search` (from Notebook 00)  
-- MAGIC **Outputs**: Curated Silver Delta tables `silver_google_search` and `silver_bing_search`  
-- MAGIC **Transformations**:
-- MAGIC - **URL Normalization**: Strips protocols (`https://`), query strings (`?utm_*`), and trailing slashes.
-- MAGIC - **Dynamic Category Extraction**: Automatically extracts the first directory path segment.
-- MAGIC - **Intent & Brand Tagging**: Classifies queries as Brand vs. Non-Brand and Short-Tail vs. Long-Tail.
-- MAGIC - **Quality Filtering**: Drops rows with null queries and ensures `impressions >= clicks`.

-- COMMAND ----------
USE CATALOG harmony_catalog;
USE SCHEMA seo_analytics;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ### 1. Clean & Enrich Google Search Data -> `silver_google_search`

-- COMMAND ----------
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
-- MAGIC ### 2. Clean & Enrich Bing Webmaster Data -> `silver_bing_search`

-- COMMAND ----------
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
    CAST(avg_ctr AS DOUBLE) AS ctr,
    CAST(avg_position AS DOUBLE) AS avg_position
FROM raw_bing_search
WHERE query IS NOT NULL;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ### 3. Verification & Cross-Engine Summary

-- COMMAND ----------
SELECT 
    'Google Silver' AS source, 
    COUNT(*) AS row_count, 
    SUM(clicks) AS total_clicks,
    SUM(impressions) AS total_impressions 
FROM silver_google_search
UNION ALL
SELECT 
    'Bing Silver' AS source, 
    COUNT(*) AS row_count, 
    SUM(clicks) AS total_clicks,
    SUM(impressions) AS total_impressions 
FROM silver_bing_search;
