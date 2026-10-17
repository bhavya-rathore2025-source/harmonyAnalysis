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
├── README.md                     # Project documentation & portfolio guide
├── requirements.txt              # Python dependencies
├── run_local_pipeline.py         # One-command local runner for testing
│
├── config/
│   ├── config.yaml               # Active website configuration (harmonybreath.com)
│   └── config.template.yaml      # Reusable template to adapt pipeline to ANY website
│
├── credentials/                  # Local folder for GSC JSON key (git-ignored)
│   └── .gitkeep
│
├── src/                          # Modular Python source code
│   ├── config.py                 # Configuration loader
│   ├── extractors/               # API clients
│   │   ├── gsc_extractor.py      # Google Search Console API client
│   │   └── bing_extractor.py     # Bing Webmaster Tools API client
│   └── transformers/
│       └── cleaner.py            # Dynamic URL normalizer & keyword categorizer
│
├── notebooks/                    # Databricks notebooks
│   └── 01_clean_and_transform_databricks.py # PySpark Silver transformation
│
├── power_bi/                     # Power BI artifacts & documentation
│   ├── README.md                 # Star-schema modeling & visual layout blueprints
│   └── measures.dax              # Production DAX measures repository
│
└── data/                         # Local data storage
    ├── raw/                      # Raw JSON extracts
    └── processed/                # Cleaned CSV/Parquet outputs
```

---

## 🔄 How Adaptability Works (Any Website in 60 Seconds)

To run this pipeline on a different website:
1. Open [`config/config.yaml`](file:///c:/Users/MY%20PC/Documents/HarmonyAnalysis/config/config.yaml).
2. Change the `website.name`, `website.domain`, and `brand_keywords`.
3. The rest of the pipeline (URL parsing, keyword categorization, table naming, and Power BI DAX) automatically adapts without modifying any Python or PySpark code.

---

## 🚀 Quick Start

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Add Credentials
- Download your Google Cloud Service Account JSON key to `credentials/gsc_service_account.json`.
- Set your Bing API Key in your environment (`set BING_API_KEY=your_key`) or inside `config/config.yaml`.

### 3. Run Pipeline Locally
```bash
python run_local_pipeline.py
```
Outputs will be generated in `data/processed/silver_google_search.csv` and `data/processed/silver_bing_search.csv`.

### 4. Open in Power BI
Follow the instructions in [`power_bi/README.md`](file:///c:/Users/MY%20PC/Documents/HarmonyAnalysis/power_bi/README.md) to link the shared date table and load the DAX measures from [`power_bi/measures.dax`](file:///c:/Users/MY%20PC/Documents/HarmonyAnalysis/power_bi/measures.dax).
