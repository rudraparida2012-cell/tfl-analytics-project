# Orchestration Flow

## Purpose
The orchestration layer automates the execution of the TfL lakehouse pipeline and ensures that dependent processing stages run in the correct order.

## Master Pipeline
Pipeline name: `pl_tfl_end_to_end`

## Current Activity Sequence
The current ADF pipeline orchestrates the following Databricks notebook activities:

1. `nb_bronze_ingestion`
2. `nb_silver_transformation`
3. `nb_gold_transformation`

## Dependency Logic
The pipeline uses success-based dependencies between activities:

- `nb_silver_transformation` runs only after `nb_bronze_ingestion` succeeds
- `nb_gold_transformation` runs only after `nb_silver_transformation` succeeds

This ensures that downstream layers do not run on failed upstream processing.

## Retry Strategy
Each Databricks notebook activity is configured with:
- retry count: 1
- retry interval: 1 minute

This helps recover from transient execution issues while still stopping the pipeline for hard failures.

## Monitoring
Pipeline execution is monitored using Azure Data Factory Monitor.

Tracked information includes:
- pipeline run status
- activity status
- activity duration
- failure messages
- execution timestamps

## Data Flow
The orchestrated data flow is:

Raw ADLS data  
→ Bronze notebook transformation  
→ Silver notebook transformation  
→ Gold notebook transformation  

## Why this matters
This orchestration layer turns separate notebook executions into a production-style workflow with dependency control, retries, and centralized monitoring.

## Current Scope
The current orchestrated implementation includes:
- Bronze layer
- Silver layer
- Gold layer

## Planned Enhancements
The following steps are planned for future orchestration enhancement:
- dbt run activity
- Power BI refresh activity
- ingestion automation inside cloud execution
- centralized pipeline log table
- automated alerting and freshness checks

## Scheduling Plan
The intended scheduling pattern is micro-batch execution on a recurring interval, such as every 15 minutes.

This matches the project design of periodic API polling and near-real-time reporting.
