# TfL Real-Time Transport Analytics Platform

## Project Overview
This project is an end-to-end Azure data engineering platform built using TfL API data. It ingests transport service data, stores it in a cloud lakehouse architecture, transforms it through Bronze, Silver, and Gold layers, models it using dbt, and serves insights through Power BI dashboards.

The project is designed to resemble a production-style data engineering solution rather than a basic portfolio demo.

---

## Business Problem
Transport operations teams and decision-makers need timely visibility into service health, line disruptions, and status changes across the network.

This project solves that problem by building a near-real-time analytics platform that:
- ingests TfL API data on a micro-batch basis
- stores historical snapshots
- transforms semi-structured API responses into structured Delta tables
- exposes business-facing reporting datasets
- enables dashboarding and operational monitoring

---

## Project Objective
Build a production-style lakehouse platform using Azure, Databricks, dbt, and Power BI to support transport status reporting and analytics.

---

## Architecture Overview
The architecture follows a medallion lakehouse pattern:

TfL API  
→ Python ingestion  
→ ADLS Gen2 Raw  
→ Databricks Bronze  
→ Databricks Silver  
→ Databricks Gold  
→ dbt semantic transformation layer  
→ Power BI dashboards  
→ ADF orchestration and monitoring  

---

## Tech Stack
- Python
- Azure Data Lake Storage Gen2
- Azure Databricks
- Delta Lake
- dbt
- Azure Data Factory
- Azure Key Vault
- Power BI
- Databricks Secret Scope
- Unity Catalog

---

## End-to-End Data Flow
1. TfL API data is extracted using Python
2. Raw JSON snapshots are landed into ADLS Gen2
3. Databricks Bronze notebooks ingest raw data into Delta format
4. Silver notebooks flatten and standardize the data
5. Gold notebooks create business-facing fact and dimension tables
6. dbt models formalize sources, staging, intermediate, and marts logic
7. Power BI consumes Gold/dbt tables for dashboarding
8. ADF orchestrates Bronze → Silver → Gold execution

---

## Medallion Architecture

### Raw Layer
Raw JSON files landed from TfL APIs into ADLS.

### Bronze Layer
Minimal transformation from raw JSON into Delta tables with ingestion metadata such as:
- bronze_loaded_at
- source_file_name
- source_system
- dataset_name

### Silver Layer
Flattened and standardized datasets for analytics-ready transformation:
- line status
- disruptions
- routes
- stop points
- arrivals

### Gold Layer
Business-facing dimensional and fact tables:
- `gold_dim_line`
- `gold_fact_line_status`
- `gold_kpi_line_status_summary`

---

## dbt Layer
dbt was used to structure the semantic transformation layer into:
- sources
- staging
- intermediate
- marts

Implemented dbt models:
- `stg_line_status`
- `int_line_status`
- `dim_line`
- `fact_line_status`
- `kpi_line_status_summary`

dbt tests were added for:
- not null
- unique
- relationships

dbt documentation and lineage were generated successfully.

---

## Power BI Layer
Power BI was connected to Databricks SQL Warehouse and used to build a semantic model and dashboard.

Implemented reporting artifacts:
- Executive Overview page
- Line Status Monitoring page
- KPI Summary page

Core Power BI measures:
- Total Status Records
- Distinct Lines
- Minor Delays Count
- Good Service Count
- Average Severity
- Distinct Affected Lines

---

## Orchestration
Azure Data Factory was used to orchestrate the lakehouse pipeline.

Master pipeline:
- `pl_tfl_end_to_end`

Current activities:
- `nb_bronze_ingestion`
- `nb_silver_transformation`
- `nb_gold_transformation`

The pipeline uses success-based dependencies, retry logic, and ADF Monitor for execution tracking.

---

## Monitoring and Logging
Monitoring was implemented through:
- ADF Monitor for activity-level pipeline execution
- Databricks notebook logs for row counts, source paths, and target paths
- validation checks to prevent zero-row outputs

Validation examples:
- Bronze row count > 0
- Silver row count > 0
- Gold row count > 0

---

## Project Structure
```text
tfl-analytics-project/
│
├── ingestion/
├── databricks/
├── dbt/
├── docs/
├── config/
├── tests/
├── data/
├── README.md
├── requirements.txt
├── .env
└── .gitignore
# tfl-analytics-project
# tfl-analytics-project
# tfl-analytics-project
