# 🚕 NYC Taxi Data ETL Pipeline (Bronze → Silver → Gold)

## Overview
This project implements an end-to-end **ETL pipeline** on the **NYC Yellow Taxi Trip dataset** using **PySpark on Databricks**, following the **Medallion Architecture (Bronze, Silver, Gold)**.

The pipeline ingests raw taxi data, cleans and validates it, and produces analytics-ready aggregated tables for reporting.

---

The pipeline is orchestrated using **Databricks Jobs** with task dependencies.

---

## Dataset
- **Source**: NYC Yellow Taxi Trip Data
- **Format**: CSV
- **Contents**: Pickup/dropoff timestamps, trip distance, fare amount, passenger count, payment type, taxes.

---

## Bronze Layer
**Purpose**: Store raw ingested data without transformations.

- Reads CSV from Databricks File System (DBFS)
- Schema inferred
- Allows nulls, duplicates, and invalid values

---

## Silver Layer
**Purpose**: Clean and prepare trusted data.

**Transformations**
- Removed null pickup/dropoff timestamps
- Filtered negative fare amounts and trip distances
- Removed duplicate records
- Added derived column `trip_duration_minutes`


---

## Gold Layer
**Purpose**: Provide analytics-ready aggregated data.

**Metrics**
- Total trips per day
- Total revenue per day
- Average fare
- Average trip distance
- Average trip duration


---

## Orchestration
- Implemented using **Databricks Jobs**
- Three tasks:
  1. Bronze ingestion
  2. Silver cleaning (depends on Bronze)
  3. Gold aggregation (depends on Silver)

---

## Technologies Used
- PySpark
- Databricks
- Delta Lake
- SQL
- Git & GitHub

---

## How to Run
1. Upload NYC Taxi CSV to DBFS
2. Run notebooks in order: Bronze → Silver → Gold  
   *or* trigger the Databricks Job
3. Query Gold tables for analytics

---

## Author
**Shruti Sharma**  
GitHub: https://github.com/shrutisharma6  
LinkedIn: https://www.linkedin.com/in/shruti-sharma-83471622b  

---

## Notes
This project demonstrates practical experience in:
- ETL pipeline design
- Medallion architecture
- PySpark transformations
- Workflow orchestration using Databricks Jobs

