-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 01. Clean & Transform (Silver Layer)
-- MAGIC **Inputs**: `raw_google_search` (includes revealed queries + NULL query for anonymized traffic), `raw_bing_search`  
-- MAGIC **Outputs**: Curated Silver Delta tables `silver_google_search` and `silver_bing_search`  
-- MAGIC **Transformations**:
-- MAGIC - **URL Normalization**: Strips protocols (`https://`), query strings (`?utm_*`), and trailing slashes.
-- MAGIC - **Dynamic Category Extraction**: Automatically extracts the first directory path segment.
-- MAGIC - **Intent & Brand Tagging**: Classifies queries as Brand vs. Non-Brand and Short-Tail vs. Long-Tail.
-- MAGIC - **Preserves NULL Queries**: Retains private/anonymized traffic with `search_query = NULL` so 100% of clicks are captured without double counting.

-- COMMAND ----------

USE CATALOG harmony_catalog;
USE SCHEMA seo_analytics;

-- COMMAND ----------

select * from harmony_catalog.seo_analytics.raw_google_search limit 100

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 1. Clean & Enrich Google Search Data -> `silver_google_search`

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import expr,col
-- MAGIC google_df= spark.table("harmony_catalog.seo_analytics.raw_google_search")
-- MAGIC google_df.limit(100).display()
-- MAGIC updated_google_df =( google_df
-- MAGIC .withColumn(
-- MAGIC     'page', 
-- MAGIC     expr("COALESCE(NULLIF(replace(page, 'https://harmonybreath.com/', ''), ''), '/')")
-- MAGIC )
-- MAGIC .withColumns({
-- MAGIC     'language': expr("""
-- MAGIC         CASE
-- MAGIC             WHEN split(page, '/')[0] IN ('es', 'de', 'fr', 'it', 'pt', 'ja')
-- MAGIC             THEN split(page, '/')[0]
-- MAGIC             ELSE 'en'
-- MAGIC         END
-- MAGIC     """),
-- MAGIC     'technique': expr("""
-- MAGIC         CASE
-- MAGIC             WHEN split(page, '/')[0] IN ('es', 'de', 'fr', 'it', 'pt', 'ja')
-- MAGIC             THEN get(split(page, '/'), 1)
-- MAGIC             ELSE get(split(page, '/'), 0)
-- MAGIC         END
-- MAGIC     """)}).drop("ctr","ingestion_timestamp"))
-- MAGIC updated_google_df.limit(100).display()
-- MAGIC
-- MAGIC updated_google_df.write.mode("overwrite").saveAsTable("harmony_catalog.seo_analytics.silver_google_search")
-- MAGIC

-- COMMAND ----------

select * from harmony_catalog.seo_analytics.silver_google_search
where page=="es/"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2. Clean & Enrich Bing Webmaster Data -> `silver_bing_search`

-- COMMAND ----------

select * from harmony_catalog.seo_analytics.raw_bing_search limit 300

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 3. Verification & Summary

-- COMMAND ----------

SELECT 
    'Google (Revealed Keywords)' AS metric, 
    COUNT(*) AS row_count, 
    SUM(clicks) AS total_clicks,
    SUM(impressions) AS total_impressions 
FROM silver_google_search
WHERE search_query IS NOT NULL
UNION ALL
SELECT 
    'Google (Anonymized / NULL Query)' AS metric, 
    COUNT(*) AS row_count, 
    SUM(clicks) AS total_clicks,
    SUM(impressions) AS total_impressions 
FROM silver_google_search
WHERE search_query IS NULL
UNION ALL
SELECT 
    'Google Total (100% Traffic)' AS metric, 
    COUNT(*) AS row_count, 
    SUM(clicks) AS total_clicks,
    SUM(impressions) AS total_impressions 
FROM silver_google_search
UNION ALL
SELECT 
    'Bing Total' AS metric, 
    COUNT(*) AS row_count, 
    SUM(clicks) AS total_clicks,
    SUM(impressions) AS total_impressions 
FROM silver_bing_search;