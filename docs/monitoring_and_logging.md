# Monitoring and Logging

## Purpose
The monitoring layer provides visibility into pipeline execution, notebook-level row counts, data freshness, and failure points.

## ADF Monitoring
ADF Monitor is used to track:
- pipeline run ID
- activity status
- activity duration
- start and end time
- failure messages

## Notebook-Level Logging
Each Databricks notebook logs:
- dataset name
- source path
- target path
- row count read
- row count written
- execution completion status

## Validation Rules
The following checks are applied:
- Bronze row count > 0
- Silver row count > 0
- Gold row count > 0

If a critical dataset writes zero rows, the notebook raises an error and the pipeline stops.

## Retry Strategy
ADF notebook activities are configured with:
- retry count: 1
- retry interval: 1 minute

Retries are intended only for transient execution issues.

## Dependency Control
Downstream activities run only if upstream notebook activities succeed:
- Bronze → Silver → Gold

## Data Freshness
Freshness is tracked using layer timestamps such as:
- bronze_loaded_at
- silver_loaded_at
- gold_loaded_at

## Future Improvements
- centralized Delta log table for pipeline execution
- automated alerting
- SLA freshness dashboard
- operational metrics dashboard
