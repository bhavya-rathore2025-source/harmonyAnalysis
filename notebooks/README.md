# Databricks SQL Notebooks Documentation

This directory contains pure SQL scripts for the Databricks Lakehouse processing stages:

---

## 1. `00_load_raw_tables.sql` (Raw Data Layer)

### Objective
- Ingest raw JSON extracts from GSC and Bing without transformations.
- Create Delta tables: `raw_google_search` and `raw_bing_search`.
- Add an `ingestion_timestamp` column for lineage and auditability.

### Catalog & Schema Setup
```sql
CREATE CATALOG IF NOT EXISTS harmony_catalog;
USE CATALOG harmony_catalog;

CREATE SCHEMA IF NOT EXISTS seo_analytics;
USE SCHEMA seo_analytics;
```
> **Note on Databricks Environment**:
> - If your Databricks workspace has **Unity Catalog** enabled, this creates `harmony_catalog.seo_analytics`.
> - If using a **free Community Edition** without Unity Catalog, replace `harmony_catalog` with `hive_metastore` or `default`.

---

## 2. `01_clean_and_transform.sql` (Silver Transformation Layer)

### Objective
- Read from `raw_google_search` and `raw_bing_search`.
- Perform data cleansing, normalization, and business enrichment.
- Output clean Delta tables: `silver_google_search` and `silver_bing_search`.

### Key SQL Transformations Applied:
1. **URL Normalization**:
   - Strips protocols (`http://`, `https://`), query parameters (`?utm_*`), and trailing slashes.
   - Ensures consistent URL keys across reports.
2. **Dynamic Section Extraction**:
   - Parses the first path directory segment (`/4-7-8-breathing/` $\rightarrow$ `4-7-8-breathing`, `/` $\rightarrow$ `home`).
3. **Keyword Classification**:
   - **Brand vs. Non-Brand**: Checks if query contains brand terms (`%harmony%`).
   - **Short-tail vs. Long-tail**: Analyzes query word count ($\le 2$ words vs. $\ge 3$ words).
4. **Data Quality Integrity**:
   - Drops null queries.
   - Enforces `impressions >= clicks`.
