
# 📊 Unified Marketing Data Platform

### *Data Integration, Automation, and Visualization using Airflow, BigQuery, and Superset*

---

## 🚀 Overview

This project addresses a major challenge faced by the client — **the absence of a unified data platform** to consolidate marketing and performance data coming from multiple sources.
Previously, data was scattered across various systems such as **Google Ads**, **Google Analytics**, **BrightLocal**, **Google My Business**, and **WhatConverts**, making it difficult to generate cohesive insights.

The solution:
A fully automated **data ingestion and analytics pipeline** built using **Python**, **Apache Airflow**, **Google BigQuery**, and **Apache Superset** — providing a single source of truth and a secure visualization platform.

---

## 🧩 Objectives

* Build a **unified marketing data warehouse**.
* Automate **data extraction, transformation, and loading (ETL)** processes.
* Enable **centralized reporting** with strong data governance and row-level security.
* Replace manual data collection and Excel-based reporting with automated dashboards.

---

## 🏗️ Architecture

Below is the high-level architecture of the platform:

```text
                ┌───────────────────────────┐
                │       Apache Airflow       │
                │   (Orchestration Layer)    │
                └────────────┬───────────────┘
                             │
    ┌────────────────────────────────────────────────────────┐
    │ Data Sources                                            │
    │  ┌──────────────┬──────────────┬──────────────┬────────┐│
    │  │ Google Ads    │ Google Analytics │ BrightLocal │ WhatConverts │
    │  └──────────────┴──────────────┴──────────────┴────────┘│
    │                  │                     │                 │
    └──────────────────┼─────────────────────┼─────────────────┘
                       │                     │
                       ▼                     ▼
               ┌──────────────────────────┐
               │        Python ETL        │
               │ (APIs, Transformations)  │
               └────────────┬─────────────┘
                            │
                            ▼
         ┌──────────────────────────────────────┐
         │         Google BigQuery              │
         │ (Centralized Data Warehouse Layer)   │
         └────────────┬────────────┬────────────┘
                      │            │
                      │            ▼
                      │  BigQuery Data Transfer
                      │  (Google Ads Native Connection)
                      │
                      ▼
           ┌──────────────────────────┐
           │     Apache Superset       │
           │  + Row Level Security     │
           │  (Dashboards & Insights)  │
           └──────────────────────────┘
```
<img width="1052" height="832" alt="image (12)" src="https://github.com/user-attachments/assets/ea7a2545-71db-4047-bca8-7a2c7d9bc5c8" />

---

## ⚙️ Workflow Summary

### 1. **Data Extraction**

* **Tools**: Python (with `requests`, `pandas`, and API SDKs)
* **Sources**:

  * Google Ads
  * Google Analytics
  * BrightLocal
  * Google My Business
  * WhatConverts
* Data fetched via REST APIs and stored temporarily as JSON/CSV.

### 2. **Data Orchestration**

* **Tool**: Apache Airflow
* **Purpose**:

  * Schedule and monitor ETL pipelines.
  * Manage data refresh cycles.
  * Handle dependency between multiple data ingestion tasks.

### 3. **Data Loading**

* **Destination**: Google BigQuery
* **Process**:

  * Google Ads data ingested using native **BigQuery Data Transfer Service**.
  * Other datasets (Analytics, BrightLocal, etc.) pushed using Python scripts via BigQuery API.

### 4. **Data Transformation**

* **Inside BigQuery**:

  * SQL-based transformations for data cleaning and schema alignment.
  * Creation of unified reporting tables (e.g., campaign performance, conversion summaries, local ranking data).

### 5. **Visualization Layer**

* **Tool**: Apache Superset
* **Purpose**:

  * Build dynamic dashboards and visual analytics for marketing KPIs.
  * Enable **Row-Level Security** to ensure users can access only permitted data (e.g., per client or region).

---

## 🧠 Tech Stack

| Component       | Technology Used                                                             | Purpose                              |
| --------------- | --------------------------------------------------------------------------- | ------------------------------------ |
| Orchestration   | **Apache Airflow**                                                          | Automate and schedule ETL workflows  |
| Data Processing | **Python**                                                                  | Fetch and transform data via APIs    |
| Data Warehouse  | **Google BigQuery**                                                         | Centralized structured storage       |
| Visualization   | **Apache Superset**                                                         | Dashboards and business intelligence |
| Security        | **Row-Level Security (RLS)**                                                | Controlled access to dashboards      |
| API Sources     | Google Ads, Google Analytics, BrightLocal, Google My Business, WhatConverts | Marketing and performance data       |

---

## 🧰 Implementation Steps

1. **Set Up Airflow DAGs**

   * Create DAGs for each source (Google Analytics, WhatConverts, etc.).
   * Define extraction, transformation, and load tasks.
   * Schedule DAGs for daily/weekly runs.

2. **Develop Python ETL Scripts**

   * Use respective APIs to fetch data.
   * Transform and normalize schema.
   * Push to BigQuery using `google-cloud-bigquery` SDK.

3. **Configure BigQuery Schema**

   * Create datasets and tables for unified schema.
   * Apply partitioning and clustering for performance.
   * Add row-level security for access control.

4. **Integrate Superset**

   * Connect Superset to BigQuery.
   * Build dashboards showing campaign KPIs, ad spend, conversions, rankings, etc.
   * Apply filters, roles, and row-level security.

---

## 📊 Example Dashboards

* Marketing Performance Overview
* Campaign ROI and Cost Analysis
* Google Ads vs. WhatConverts Conversion Comparison
* Local SEO (BrightLocal) Ranking Trends
* Traffic and Engagement (Google Analytics)

---

## 📅 Scheduling & Automation

* **Apache Airflow DAGs** automate:

  * Daily ingestion from APIs.
  * Weekly summary table refreshes.
  * Email alerts for pipeline failures.

---

## 🔒 Data Governance

* **Row-Level Security (RLS)** ensures:

  * Users see only their organization’s data.
  * Multi-client access without data leakage.
  * Compliance with data privacy requirements.

