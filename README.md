# 🚀 UnifiedCommerce: M&A Data Integration Pipeline

![Spark](https://img.shields.io/badge/Apache_Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)
![AWS](https://img.shields.io/badge/AWS-232F3E?style=for-the-badge&logo=amazon-aws&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-D05C4B?style=for-the-badge&logo=deltalake&logoColor=white)

> **An End-to-End Data Engineering solution harmonizing conflicting schemas from a corporate acquisition into a unified Delta Lakehouse.**

---

## 📖 Project Overview

**The Business Scenario:**
A Parent Company has recently acquired a smaller "Child Company." The Parent Company needs to integrate the Child Company's sales and customer data into their central Data Warehouse to generate global insights.

**The Challenge:**
The Child Company's data, stored in **AWS S3**, is incompatible with the Parent Company's standards:
* **Schema Mismatch:** Different column names (`id` vs `customer_code`).
* **Data Quality Issues:** Null values in critical fields (e.g., `City`), inconsistent casing in names.
* **Scalability:** Need a pipeline that handles initial history load AND future incremental updates without duplicates.

**The Solution:**
I engineered a scalable ETL pipeline using **Databricks (PySpark)** and **Delta Lake** that ingests raw data, harmonizes the schema, enriches missing information using lookup tables, and merges the result into the Parent's Gold layer using **Upsert (SCD Type 1)** logic.

---

## 🏗️ Technical Architecture

![Architecture Diagram](images/architecture_diagram.png)
*(Note: This is the workflow I designed. Data flows from AWS S3 -> Bronze (Raw) -> Silver (Cleaned/Enriched) -> Gold (Aggregated/Modeled) -> Dashboard)*

### **The Medallion Architecture Implementation**

| Layer | Type | Responsibility | Key Tech Used |
| :--- | :--- | :--- | :--- |
| **Bronze** | Raw | Ingests CSV files from **AWS S3** with zero transformation. Captures file metadata (`_metadata.file_name`). | `spark.read.csv`, Wildcard Paths |
| **Silver** | Cleaned | **Schema Harmonization:** Renames columns to match Parent schema.<br>**Data Patching:** Fills `NULL` cities using a manual dictionary lookup.<br>**Standardization:** Applies Title Casing (`initcap`) to names. | `join`, `coalesce`, `withColumn`, `initcap` |
| **Gold** | Curated | **Upsert Logic:** Merges data into the Master Dimension tables. Updates existing records and inserts new ones. | `DeltaTable.merge`, `whenMatchedUpdate`, `whenNotMatchedInsert` |

---

## 🛠️ Tech Stack

* **Cloud Storage:** AWS S3 (Source Data)
* **Compute:** Databricks (Standard Cluster, Runtime 14.3 LTS)
* **Engine:** Apache Spark (PySpark)
* **Storage Format:** Delta Lake (for ACID Transactions & Time Travel)
* **Catalog:** Unity Catalog (3-Level Namespace: `catalog.schema.table`)
* **Orchestration:** Databricks Workflows
* **Visualization:** PowerBI / Databricks SQL Dashboards

---

## 💻 Key Engineering Features

### 1. Dynamic Ingestion via Widgets
Instead of hardcoding paths, I utilized Databricks Widgets to parameterize the notebook, making it reusable for different datasets (e.g., `Sales`, `Customers`).

```python
# Setup dynamic widgets for pipeline flexibility
dbutils.widgets.text("data_source", "customers", "Data Source")
data_source = dbutils.widgets.get("data_source")

base_path = f's3://sportsbar-child/{data_source}/*.csv'
