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
-- MAGIC import os, yaml
-- MAGIC from pyspark.sql.functions import expr,col
-- MAGIC from delta.tables import DeltaTable
-- MAGIC
-- MAGIC lookback_days = 64
-- MAGIC config_path = "../config/config.yaml"
-- MAGIC if os.path.exists(config_path):
-- MAGIC     with open(config_path, "r", encoding="utf-8") as f:
-- MAGIC         lookback_days = yaml.safe_load(f).get("pipeline", {}).get("lookback_days", 64)
-- MAGIC
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
-- MAGIC silver_google_table = "harmony_catalog.seo_analytics.silver_google_search"
-- MAGIC if lookback_days > 7 or not spark.catalog.tableExists(silver_google_table):
-- MAGIC     updated_google_df.dropDuplicates(["date", "page", "query"]).write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(silver_google_table)
-- MAGIC     print(f"Overwrote {silver_google_table} (lookback_days = {lookback_days} > 7)")
-- MAGIC else:
-- MAGIC     delta_silver_google = DeltaTable.forName(spark, silver_google_table)
-- MAGIC     delta_silver_google.alias("tgt").merge(
-- MAGIC         updated_google_df.dropDuplicates(["date", "page", "query"]).alias("src"),
-- MAGIC         """
-- MAGIC         tgt.date = src.date AND 
-- MAGIC         tgt.page = src.page AND 
-- MAGIC         (tgt.query = src.query OR (tgt.query IS NULL AND src.query IS NULL))
-- MAGIC         """
-- MAGIC     ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
-- MAGIC     print(f"Merged into {silver_google_table}")



-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2. Clean & Enrich Bing Webmaster Data -> `silver_bing_search`

-- COMMAND ----------

select * from harmony_catalog.seo_analytics.raw_bing_page_search limit 100

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import expr
-- MAGIC bing_page_df= spark.table('harmony_catalog.seo_analytics.raw_bing_page_search')
-- MAGIC updated_bing_page_df= (
-- MAGIC     bing_page_df.withColumn("page", expr("COALESCE(NULLIF(parse_url(page, 'PATH'), ''), '/')"))
-- MAGIC .withColumns({
-- MAGIC     'language': expr("""
-- MAGIC         CASE
-- MAGIC             WHEN split(page, '/')[1] IN ('es', 'de', 'fr', 'it', 'pt', 'ja')
-- MAGIC             THEN split(page, '/')[1]
-- MAGIC             ELSE 'en'
-- MAGIC         END
-- MAGIC     """),
-- MAGIC     'technique': expr("""
-- MAGIC         CASE
-- MAGIC             WHEN split(page, '/')[1] IN ('es', 'de', 'fr', 'it', 'pt', 'ja')
-- MAGIC             THEN get(split(page, '/'), 2)
-- MAGIC             ELSE get(split(page, '/'), 1)
-- MAGIC         END
-- MAGIC     """)}
-- MAGIC ))
-- MAGIC updated_bing_page_df= updated_bing_page_df.drop("ingestion_timestamp")
-- MAGIC
-- MAGIC silver_bing_page_table = "harmony_catalog.seo_analytics.silver_bing_page_search"
-- MAGIC if lookback_days > 7 or not spark.catalog.tableExists(silver_bing_page_table):
-- MAGIC     updated_bing_page_df.dropDuplicates(["date", "page"]).write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(silver_bing_page_table)
-- MAGIC     print(f"Overwrote {silver_bing_page_table} (lookback_days = {lookback_days} > 7)")
-- MAGIC else:
-- MAGIC     delta_bing_page = DeltaTable.forName(spark, silver_bing_page_table)
-- MAGIC     delta_bing_page.alias("tgt").merge(
-- MAGIC         updated_bing_page_df.dropDuplicates(["date", "page"]).alias("src"),
-- MAGIC         "tgt.date = src.date AND tgt.page = src.page"
-- MAGIC     ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
-- MAGIC     print(f"Merged into {silver_bing_page_table}")
-- MAGIC
-- MAGIC bing_search_df= spark.table("harmony_catalog.seo_analytics.raw_bing_search")
-- MAGIC bing_search_df=bing_search_df.drop("ingestion_timestamp")
-- MAGIC
-- MAGIC silver_bing_search_table = "harmony_catalog.seo_analytics.silver_bing_search"
-- MAGIC if lookback_days > 7 or not spark.catalog.tableExists(silver_bing_search_table):
-- MAGIC     bing_search_df.dropDuplicates(["date", "query"]).write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(silver_bing_search_table)
-- MAGIC     print(f"Overwrote {silver_bing_search_table} (lookback_days = {lookback_days} > 7)")
-- MAGIC else:
-- MAGIC     delta_bing_search = DeltaTable.forName(spark, silver_bing_search_table)
-- MAGIC     delta_bing_search.alias("tgt").merge(
-- MAGIC         bing_search_df.dropDuplicates(["date", "query"]).alias("src"),
-- MAGIC         "tgt.date = src.date AND tgt.query = src.query"
-- MAGIC     ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
-- MAGIC     print(f"Merged into {silver_bing_search_table}")


-- COMMAND ----------

create or replace table harmony_catalog.seo_analytics.bing_latest_page
SELECT * 
FROM harmony_catalog.seo_analytics.silver_bing_page_search
WHERE date = (SELECT MAX(date) FROM harmony_catalog.seo_analytics.silver_bing_page_search);
create or replace table harmony_catalog.seo_analytics.bing_latest
SELECT * 
FROM harmony_catalog.seo_analytics.silver_bing_search
WHERE date = (SELECT MAX(date) FROM harmony_catalog.seo_analytics.silver_bing_search);
create or replace table harmony_catalog.seo_analytics.google_latest
SELECT * 
FROM harmony_catalog.seo_analytics.silver_google_search
WHERE date = (SELECT MAX(date) FROM harmony_catalog.seo_analytics.silver_google_search);

-- COMMAND ----------

