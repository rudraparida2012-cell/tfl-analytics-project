# Databricks notebook source
# MAGIC %md
# MAGIC # TfL Gold Layer in Databricks — Correct Approach
# MAGIC
# MAGIC This notebook builds the **Gold layer** from your Silver Delta files.
# MAGIC
# MAGIC ## What this notebook does
# MAGIC 1. Reads Silver Delta datasets from ADLS  
# MAGIC 2. Builds a clean dimension table for lines  
# MAGIC 3. Builds a fact table for line status  
# MAGIC 4. Builds a KPI summary table for reporting  
# MAGIC 5. Writes Delta files into the `gold` container  
# MAGIC 6. Validates the Gold output  
# MAGIC
# MAGIC ## Current focus
# MAGIC This notebook starts with:
# MAGIC - `gold_dim_line`
# MAGIC - `gold_fact_line_status`
# MAGIC - `gold_kpi_line_status_summary`
# MAGIC
# MAGIC These are enough to support:
# MAGIC - Power BI semantic modeling
# MAGIC - line-level status analytics
# MAGIC - executive KPI reporting

# COMMAND ----------

# MAGIC %md
# MAGIC ## Why Gold exists
# MAGIC Gold is the business-facing layer.
# MAGIC
# MAGIC Gold should contain:
# MAGIC - fact tables
# MAGIC - dimension tables
# MAGIC - KPI-ready aggregated tables
# MAGIC
# MAGIC Gold is where the data becomes ready for:
# MAGIC - dashboards
# MAGIC - reporting
# MAGIC - stakeholder consumption
# MAGIC - downstream semantic models

# COMMAND ----------

# === Step 1: Set your configuration ===

storage_account_name = "sttflanalyticsdevrudx"   # change if needed
scope_name = "kv-tfl-scope"                      # change if needed
secret_key_name = "adls-storage-key"             # change if needed

# COMMAND ----------

# === Step 2: Read storage key from Databricks secret scope ===

storage_account_key = dbutils.secrets.get(
    scope=scope_name,
    key=secret_key_name
)

print("Storage account name loaded:", storage_account_name)
print("Secret scope loaded successfully.")

# COMMAND ----------

# === Step 3: Configure Spark for ADLS access ===

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key
)

print("Spark ADLS configuration applied.")

# COMMAND ----------

# === Step 4: Define Silver and Gold paths ===

silver_line_status_path = f"abfss://silver@{storage_account_name}.dfs.core.windows.net/tfl/line_status/"

gold_dim_line_path = f"abfss://gold@{storage_account_name}.dfs.core.windows.net/tfl/dim_line/"
gold_fact_line_status_path = f"abfss://gold@{storage_account_name}.dfs.core.windows.net/tfl/fact_line_status/"
gold_kpi_line_status_summary_path = f"abfss://gold@{storage_account_name}.dfs.core.windows.net/tfl/kpi_line_status_summary/"

print("Silver path:", silver_line_status_path)
print("Gold dim path:", gold_dim_line_path)
print("Gold fact path:", gold_fact_line_status_path)
print("Gold KPI path:", gold_kpi_line_status_summary_path)

# COMMAND ----------

# === Step 5: Read Silver line_status data ===

df_line_status_silver = spark.read.format("delta").load(silver_line_status_path)

display(df_line_status_silver)

# COMMAND ----------

# === Step 6: Inspect Silver schema ===

df_line_status_silver.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC # Gold Table 1 — `gold_dim_line`
# MAGIC
# MAGIC This dimension stores one record per TfL line.
# MAGIC
# MAGIC Expected columns:
# MAGIC - `line_id`
# MAGIC - `line_name`
# MAGIC - `mode_name`
# MAGIC - `gold_loaded_at`

# COMMAND ----------

# === Step 7: Build line dimension ===

from pyspark.sql.functions import current_timestamp, col

df_gold_dim_line = (
    df_line_status_silver
    .select(
        col("line_id"),
        col("line_name"),
        col("mode_name")
    )
    .dropDuplicates(["line_id"])
    .withColumn("gold_loaded_at", current_timestamp())
)

display(df_gold_dim_line)

# COMMAND ----------

print("Starting Gold build for dim_line")
print("Gold output path:", gold_dim_line_path)

gold_count = df_gold_dim_line.count()
print("Gold dim_line row count before write:", gold_count)

if gold_count == 0:
    raise ValueError("Gold dim_line has zero rows")

(
    df_gold_dim_line.write
    .format("delta")
    .mode("overwrite")
    .save(gold_dim_line_path)
)

df_gold_dim_line_check = spark.read.format("delta").load(gold_dim_line_path)
written_count = df_gold_dim_line_check.count()

print("Gold dim_line row count after write:", written_count)
print("Gold build completed successfully for dim_line")

display(df_gold_dim_line_check.limit(20))

# COMMAND ----------

# === Step 9: Validate line dimension ===

df_gold_dim_line_check = spark.read.format("delta").load(gold_dim_line_path)
display(df_gold_dim_line_check)

# COMMAND ----------

# MAGIC %md
# MAGIC # Gold Table 2 — `gold_fact_line_status`
# MAGIC
# MAGIC This fact table stores one row per line status record.
# MAGIC
# MAGIC Expected columns:
# MAGIC - `line_id`
# MAGIC - `line_name`
# MAGIC - `mode_name`
# MAGIC - `line_status_id`
# MAGIC - `status_line_id`
# MAGIC - `status_severity`
# MAGIC - `status_severity_description`
# MAGIC - `status_reason`
# MAGIC - `status_created_at`
# MAGIC - `line_modified_at`
# MAGIC - `gold_loaded_at`

# COMMAND ----------

# === Step 10: Build fact_line_status ===

from pyspark.sql.functions import current_timestamp

df_gold_fact_line_status = (
    df_line_status_silver
    .select(
        "line_id",
        "line_name",
        "mode_name",
        "line_status_id",
        "status_line_id",
        "status_severity",
        "status_severity_description",
        "status_reason",
        "status_created_at",
        "line_modified_at"
    )
    .withColumn("gold_loaded_at", current_timestamp())
)

display(df_gold_fact_line_status)

# COMMAND ----------

print("Starting Gold build for fact_line_status")
print("Gold output path:", gold_fact_line_status_path)

gold_count = df_gold_fact_line_status.count()
print("Gold fact_line_status row count before write:", gold_count)

if gold_count == 0:
    raise ValueError("Gold fact_line_status has zero rows")

(
    df_gold_fact_line_status.write
    .format("delta")
    .mode("overwrite")
    .save(gold_fact_line_status_path)
)

df_gold_fact_line_status_check = spark.read.format("delta").load(gold_fact_line_status_path)
written_count = df_gold_fact_line_status_check.count()

print("Gold fact_line_status row count after write:", written_count)
print("Gold build completed successfully for fact_line_status")

display(df_gold_fact_line_status_check.limit(20))

# COMMAND ----------

# === Step 12: Validate fact_line_status ===

df_gold_fact_line_status_check = spark.read.format("delta").load(gold_fact_line_status_path)
display(df_gold_fact_line_status_check)

# COMMAND ----------

# MAGIC %md
# MAGIC # Gold Table 3 — `gold_kpi_line_status_summary`
# MAGIC
# MAGIC This aggregated KPI table supports dashboards.
# MAGIC
# MAGIC Example metrics:
# MAGIC - number of status records
# MAGIC - number of distinct lines
# MAGIC - counts by severity

# COMMAND ----------

# === Step 13: Build KPI summary ===

from pyspark.sql.functions import count, countDistinct

df_gold_kpi_line_status_summary = (
    df_gold_fact_line_status
    .groupBy("status_severity", "status_severity_description")
    .agg(
        count("*").alias("status_record_count"),
        countDistinct("line_id").alias("distinct_line_count")
    )
    .withColumn("gold_loaded_at", current_timestamp())
)

display(df_gold_kpi_line_status_summary)

# COMMAND ----------

print("Starting Gold build for kpi_line_status_summary")
print("Gold output path:", gold_kpi_line_status_summary_path)

gold_count = df_gold_kpi_line_status_summary.count()
print("Gold kpi_line_status_summary row count before write:", gold_count)

if gold_count == 0:
    raise ValueError("Gold kpi_line_status_summary has zero rows")

(
    df_gold_kpi_line_status_summary.write
    .format("delta")
    .mode("overwrite")
    .save(gold_kpi_line_status_summary_path)
)

df_gold_kpi_line_status_summary_check = spark.read.format("delta").load(gold_kpi_line_status_summary_path)
written_count = df_gold_kpi_line_status_summary_check.count()

print("Gold kpi_line_status_summary row count after write:", written_count)
print("Gold build completed successfully for kpi_line_status_summary")

display(df_gold_kpi_line_status_summary_check.limit(20))

# COMMAND ----------

# === Step 15: Validate KPI summary ===

df_gold_kpi_line_status_summary_check = spark.read.format("delta").load(gold_kpi_line_status_summary_path)
display(df_gold_kpi_line_status_summary_check)

# COMMAND ----------

# MAGIC %md
# MAGIC # Optional: create temp views for quick SQL analysis

# COMMAND ----------

df_gold_dim_line_check.createOrReplaceTempView("vw_gold_dim_line")
df_gold_fact_line_status_check.createOrReplaceTempView("vw_gold_fact_line_status")
df_gold_kpi_line_status_summary_check.createOrReplaceTempView("vw_gold_kpi_line_status_summary")

display(spark.sql("SELECT * FROM vw_gold_dim_line LIMIT 10"))
display(spark.sql("SELECT * FROM vw_gold_fact_line_status LIMIT 10"))
display(spark.sql("SELECT * FROM vw_gold_kpi_line_status_summary LIMIT 10"))

# COMMAND ----------

from datetime import datetime
print("Notebook execution finished at:", datetime.utcnow())

# COMMAND ----------

# MAGIC %md
# MAGIC # Suggested next Gold tables
# MAGIC
# MAGIC After these three, you can add:
# MAGIC - `gold_fact_disruptions`
# MAGIC - `gold_fact_arrivals`
# MAGIC - `gold_dim_stop_point`
# MAGIC - `gold_kpi_arrival_summary`
# MAGIC - `gold_kpi_disruption_summary`

# COMMAND ----------

# MAGIC %md
# MAGIC # Interview explanation
# MAGIC
# MAGIC You can describe the Gold step like this:
# MAGIC
# MAGIC - Silver created clean, reusable datasets
# MAGIC - Gold transformed those into business-facing facts, dimensions, and KPI summaries
# MAGIC - The Gold layer is optimized for Power BI and stakeholder reporting
# MAGIC - This separates raw/technical processing from reporting semantics