# TfL Real-Time Transport Analytics Platform

## Project Overview
This project is an end-to-end Azure data engineering platform built using TfL API data. It ingests transport service data directly into Azure Data Lake Storage Gen2, processes it through a Databricks medallion architecture, models it with dbt, and serves reporting datasets for Power BI.

The project is designed to resemble a production-style Azure lakehouse solution rather than a simple notebook demo.

---

## Business Problem
Transport operations teams and decision-makers need timely visibility into service health, line disruptions, and status changes across the network.

This project solves that problem by building a near-real-time analytics platform that:

- pulls TfL API data on a recurring schedule
- stores historical raw snapshots in cloud storage
- transforms semi-structured JSON into structured Delta tables
- serves business-ready analytical datasets
- supports orchestration, monitoring, and reporting

---

## Project Objective
Build a cloud-orchestrated Azure lakehouse pipeline using TfL API data, Azure Data Lake Storage Gen2, Azure Databricks, dbt, Azure Data Factory, and Power BI.

---

## Final End-to-End Architecture
The final architecture follows a cloud-driven micro-batch lakehouse design:

TfL API  
→ ADF Schedule Trigger (every 15 minutes)  
→ Databricks ingestion notebook  
→ ADLS Gen2 Raw  
→ Databricks Bronze  
→ Databricks Silver  
→ Databricks Gold  
→ dbt semantic layer  
→ Power BI refresh  
→ dashboard reporting

---

## Architecture Pattern
This project uses:

- **processing type:** batch
- **execution pattern:** micro-batch
- **load pattern:** incremental raw snapshot ingestion
- **storage pattern:** medallion architecture
- **serving pattern:** semantic model + BI reporting

It is not a true streaming architecture because processing is triggered on a schedule, not by a continuously running event stream.

---

## Tech Stack
- Python
- Azure Data Lake Storage Gen2
- Azure Databricks
- Delta Lake
- dbt
- Azure Data Factory
- Azure Key Vault / Databricks Secret Scope
- Power BI
- Databricks SQL Warehouse
- GitHub
- Databricks Git Folder integration

---

## End-to-End Data Flow

### 1. Ingestion
A Databricks notebook pulls data from TfL API endpoints and writes raw JSON snapshots directly into the `raw` ADLS container.

Examples of ingested datasets:
- line status
- disruptions
- routes
- stoppoints
- arrivals

Files are stored as timestamped raw snapshots in ADLS.

### 2. Bronze Layer
The Bronze layer performs minimal transformation from raw JSON into Delta format. It preserves source structure while adding ingestion metadata such as:

- `source_file_name`
- `source_system`
- `dataset_name`
- `bronze_loaded_at`

### 3. Silver Layer
The Silver layer standardizes and flattens Bronze data into analytics-ready structures.

For example, line status nested JSON is flattened into structured fields such as:
- line id
- line name
- mode name
- status severity
- status description
- timestamps

### 4. Gold Layer
The Gold layer creates business-facing marts for analytics and reporting.

Implemented Gold outputs:
- `dim_line`
- `fact_line_status`
- `kpi_line_status_summary`

### 5. dbt Layer
dbt formalizes the semantic transformation layer and provides:
- model organization
- dependency management
- data testing
- documentation
- lineage

Implemented dbt models:
- `stg_line_status`
- `int_line_status`
- `dim_line`
- `fact_line_status`
- `kpi_line_status_summary`

### 6. Power BI
Power BI consumes the final curated analytical model for dashboarding and KPI reporting.

---

## Medallion Architecture

### Raw Layer
Raw JSON snapshots from TfL APIs are written directly into ADLS Gen2.

Purpose:
- preserve source payloads
- support replay/reprocessing
- enable auditability
- maintain historical snapshots

### Bronze Layer
Bronze stores raw data in Delta format with minimal transformation.

Purpose:
- standardize storage format
- retain original payload
- append lineage metadata

### Silver Layer
Silver applies schema standardization, flattening, and cleansing.

Purpose:
- turn nested API structures into structured analytical records
- normalize fields for business use
- prepare for Gold marts

### Gold Layer
Gold produces reporting-ready dimension, fact, and KPI tables.

Purpose:
- enable Power BI consumption
- support semantic modeling
- provide business-friendly reporting datasets

---

## Implemented Datasets

### Raw / Bronze
- `line_status`
- `disruptions`
- `routes`
- `stoppoints`
- `arrivals`

### Silver
- `line_status`

### Gold
- `dim_line`
- `fact_line_status`
- `kpi_line_status_summary`

### dbt
- `stg_line_status`
- `int_line_status`
- `dim_line`
- `fact_line_status`
- `kpi_line_status_summary`

---

## dbt Layer Details
dbt is used to create a formal semantic layer on top of curated Databricks tables.

### Implemented capabilities
- source definitions
- staging models
- marts models
- not null tests
- unique tests
- relationship tests
- dbt docs generation
- lineage graph generation

### dbt execution
dbt is executed from a Databricks notebook inside the orchestrated pipeline using:

- Git-backed dbt project in Databricks
- generated `profiles.yml`
- Databricks SQL Warehouse connection
- Databricks PAT stored in secret scope

---

## Power BI Layer

### Semantic model
A semantic model was created in Power BI using the final Gold/dbt outputs.

### Model entities
- `dim_line`
- `fact_line_status`
- `kpi_line_status_summary`

### Example measures
- Total Status Records
- Distinct Lines
- Distinct Affected Lines
- Good Service Count
- Minor Delays Count
- Average Severity

### Reporting purpose
The Power BI layer provides:
- executive summary reporting
- operational line status monitoring
- KPI summaries by severity

---

## Orchestration

### Master Pipeline
Pipeline name:
- `pl_tfl_end_to_end`

### Final activity flow
- `nb_ingest_tfl_to_raw`
- `nb_bronze_ingestion`
- `nb_silver_transformation`
- `nb_gold_transformation`
- `nb_dbt_run`
- `nb_pbi_refresh`

### Trigger
A scheduled ADF trigger runs the pipeline every **15 minutes**.

### Dependency logic
Each step runs only after the previous one succeeds.

Flow:
- ingestion → bronze → silver → gold → dbt → Power BI refresh

### Retry logic
Notebook activities are configured with:
- retry count: 1
- retry interval: 60 seconds

---

## Monitoring and Logging

### ADF monitoring
ADF Monitor is used to track:
- pipeline run status
- trigger run status
- activity run status
- execution duration
- start and end timestamps
- failure messages

### Notebook logging
Databricks notebooks log:
- dataset name
- source path
- target path
- input row counts
- output row counts
- completion status

### Validation rules
Basic validation checks are included, such as:
- Bronze row count > 0
- Silver row count > 0
- Gold row count > 0

If a critical transformation produces zero rows, the notebook raises an error and the pipeline stops.

---

## Security and Secrets
Secrets are managed using Databricks secret scope / Azure Key Vault integration.

Examples of managed secrets:
- ADLS storage key
- TfL API key
- Databricks PAT
- Power BI credentials

No secrets or tokens are stored directly in source control.

---

## Git Integration
The project repository is stored in GitHub and cloned into Databricks using a Git folder.

This enables:
- version control
- dbt project execution from Databricks
- easier project packaging and collaboration

---

## Project Structure

```text
tfl-analytics-project/
│
├── data/
│   └── raw/
├── dbt/
│   ├── models/
│   ├── macros/
│   ├── tests/
│   ├── dbt_project.yml
│   └── packages.yml
├── docs/
│   ├── data_model_inventory.md
│   ├── monitoring_and_logging.md
│   ├── orchestration_flow.md
│   ├── cv_project_summary.md
│   ├── linkedin_project_summary.md
│   └── interview_project_pitch.md
├── ingestion/
├── README.md
├── requirements.txt
└── .gitignore
