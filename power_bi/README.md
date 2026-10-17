# Power BI Implementation Guide

This guide details how to model the two Databricks silver tables (`silver_google_search` and `silver_bing_search`) and build the executive visualization suite.

---

## 1. Connecting Power BI to Databricks

1. In Power BI Desktop, click **Get Data** $\rightarrow$ Search for **Databricks**.
2. Enter your **Server Hostname** and **HTTP Path** (from your Databricks SQL Warehouse / Compute details).
3. Select **Import** mode.
4. Check both tables: `silver_google_search` and `silver_bing_search`.

---

## 2. Setting Up the Shared Date Model

In Power BI, create a new calculated table for your conformed calendar:

```dax
Dim_Date = 
VAR MinDate = MIN(MIN(silver_google_search[date]), MIN(silver_bing_search[date]))
VAR MaxDate = MAX(MAX(silver_google_search[date]), MAX(silver_bing_search[date]))
RETURN
ADDCOLUMNS(
    CALENDAR(MinDate, MaxDate),
    "Year", YEAR([Date]),
    "Month Number", MONTH([Date]),
    "Month Name", FORMAT([Date], "mmm yyyy"),
    "Quarter", "Q" & FORMAT([Date], "q")
)
```

### Establish Relationships:
- `Dim_Date[Date]` $\rightarrow$ `silver_google_search[date]` (1-to-many, Single cross-filter)
- `Dim_Date[Date]` $\rightarrow$ `silver_bing_search[date]` (1-to-many, Single cross-filter)

> **Result**: Any date slicer on any report page automatically filters both Google and Bing data simultaneously!

---

## 3. Visualization Blueprints (3 Dedicated Pages)

### Page 1: Executive Search Performance (Head-to-Head Comparison)
* **Top Banner KPI Cards**:
  - `Total Search Clicks` (with MoM % indicator)
  - `Total Search Impressions`
  - `Blended CTR`
  - `Google Click Share %` vs. `Bing Click Share %`
* **Dual Trend Area Chart**:
  - **X-axis**: `Dim_Date[Date]`
  - **Values**: `[Google Clicks]` and `[Bing Clicks]`
* **Traffic by Website Section (Clustered Bar Chart)**:
  - **Y-axis**: `page_category`
  - **X-axis**: `[Google Clicks]` and `[Bing Clicks]`
* **Brand vs. Non-Brand Split (Donut Chart)**:
  - **Legend**: `brand_type`
  - **Values**: `[Total Search Clicks]`

---

### Page 2: Google Search Console Intelligence
* **Striking Distance Scatter Plot (High-Value SEO Visual)**:
  - **X-axis**: `[Google Weighted Avg Position]` (Set axis to **Reversed**, 1 to 20)
  - **Y-axis**: `[Google Impressions]`
  - **Bubble Size**: `[Google Clicks]`
  - **Details**: `search_query`
* **Keyword Performance Table**:
  - `search_query`, `landing_page`, `[Google Impressions]`, `[Google Clicks]`, `[Google CTR]`, `[Google Weighted Avg Position]`
  - In-cell data bars for Impressions and a gradient background for CTR.

---

### Page 3: Bing Webmaster Intelligence
* Similar deep-dive layout showing queries, categories, and positions unique to Bing traffic.
