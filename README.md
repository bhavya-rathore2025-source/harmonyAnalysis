# Organic Search Intelligence: End-to-End Analytics Pipeline

A production-grade, adaptable Data Engineering & BI project that ingests search performance data from **Google Search Console** and **Bing Webmaster Tools**, cleans and enriches it via **Databricks / PySpark**, and delivers an executive dashboard suite in **Power BI**.

---

## 🌟 Architecture Overview

```
[ External APIs ]
├── Google Search Console API (OAuth/Service Account)
└── Bing Webmaster Tools API (REST / API Key)
           │
           ▼
[ Configuration & Ingestion Engine ] (src/extractors)
├── Dynamic URL Normalizer & Category Parser
└── Brand vs. Non-Brand & Keyword Length Classifier
           │
           ▼
[ Databricks Lakehouse: Silver Layer ] (notebooks/01_clean_and_transform_databricks.py)
├── silver_google_search (Cleaned, enriched Delta table)
└── silver_bing_search   (Cleaned, enriched Delta table)
           │
           ▼ (Databricks Connector / DirectQuery / Import)
[ Power BI Semantic Model & Visual Dashboards ] (power_bi/)
├── Shared Conformed Dim_Date Dimension (Unified slicers)
├── 3-Page Executive & Operational Dashboards
└── Advanced SEO Opportunity Matrix (Striking Distance Analysis)
```

---

## 📁 Project Directory Structure

```text
HarmonyAnalysis/
├── .gitignore                    # Protects secrets (.json, .env, keys)
├── README.md                     # Project documentation
├── requirements.txt              # Python dependencies
│
├── config/
│   ├── config.yaml               # Active website configuration (harmonybreath.com)
│   └── config.template.yaml      # Reusable template to adapt pipeline to ANY website
│
├── credentials/                  # Local folder for GSC JSON key (git-ignored)
│   └── .gitkeep
│
├── notebooks/                    # Databricks notebooks
│   ├── 00_extract_api_to_raw.py  # Ingests API data to Bronze Delta tables
│   └── 01_clean_and_transform.sql# Databricks SQL Silver transformation layer
│
└── power_bi/                     # Power BI artifacts & documentation
    ├── README.md                 # Star-schema modeling & visual layout blueprints
    └── measures.dax              # Production DAX measures repository
```

---

## 🔄 How Adaptability Works (Any Website in 60 Seconds)

To run this pipeline on a different website:
1. Open [`config/config.yaml`](file:///c:/Users/MY%20PC/Documents/HarmonyAnalysis/config/config.yaml).
2. Change the `website.name`, `website.domain`, and `brand_keywords`.
3. Databricks and Power BI adapt to your target site settings.

---

## 🚀 Quick Start

### 1. Credentials Setup
- Add your Google Cloud Service Account JSON key to `credentials/`.
- Add your Bing API Key to Databricks Secrets or `.env`.

### 2. Run in Databricks
1. Run [`notebooks/00_extract_api_to_raw.py`](file:///c:/Users/MY%20PC/Documents/HarmonyAnalysis/notebooks/00_extract_api_to_raw.py) to ingest API data into Bronze Delta tables.
2. Run [`notebooks/01_clean_and_transform.sql`](file:///c:/Users/MY%20PC/Documents/HarmonyAnalysis/notebooks/01_clean_and_transform.sql) to clean and transform into Silver Delta tables.

### 3. Open in Power BI
Follow the instructions in [`power_bi/README.md`](file:///c:/Users/MY%20PC/Documents/HarmonyAnalysis/power_bi/README.md) to connect to Databricks and load the DAX measures from [`power_bi/measures.dax`](file:///c:/Users/MY%20PC/Documents/HarmonyAnalysis/power_bi/measures.dax).
