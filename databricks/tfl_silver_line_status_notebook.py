# Databricks notebook source
# MAGIC %md
# MAGIC # TfL Silver Layer in Databricks — Correct Approach
# MAGIC
# MAGIC This notebook builds the **Silver layer** from your Bronze Delta files.
# MAGIC
# MAGIC ## What this notebook does
# MAGIC 1. Reads Bronze Delta data from ADLS  
# MAGIC 2. Flattens nested fields for `line_status`  
# MAGIC 3. Standardizes timestamps  
# MAGIC 4. Selects analytical columns  
# MAGIC 5. Adds Silver metadata columns  
# MAGIC 6. Writes Delta files into the `silver` container  
# MAGIC 7. Validates the Silver output  
# MAGIC
# MAGIC ## Current focus
# MAGIC This notebook starts with:
# MAGIC
# MAGIC - `line_status`
# MAGIC
# MAGIC Once this works, you can follow the same pattern for:
# MAGIC - `disruptions`
# MAGIC - `routes`
# MAGIC - `stoppoints`
# MAGIC - `arrivals`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Why Silver exists
# MAGIC Silver is the layer where data becomes:
# MAGIC
# MAGIC - cleaner
# MAGIC - standardized
# MAGIC - easier to query
# MAGIC - more reusable for Gold and Power BI
# MAGIC
# MAGIC In Silver, you are allowed to:
# MAGIC - flatten nested JSON
# MAGIC - cast timestamps and types
# MAGIC - rename columns
# MAGIC - remove technical noise
# MAGIC - keep business-relevant fields
# MAGIC
# MAGIC You should still avoid:
# MAGIC - heavy aggregations
# MAGIC - KPI calculations
# MAGIC - semantic marts

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

# === Step 4: Define Bronze and Silver paths for line_status ===

bronze_line_status_path = f"abfss://bronze@{storage_account_name}.dfs.core.windows.net/tfl/line_status/"
silver_line_status_path = f"abfss://silver@{storage_account_name}.dfs.core.windows.net/tfl/line_status/"

print("Bronze path:", bronze_line_status_path)
print("Silver path:", silver_line_status_path)

# COMMAND ----------

# === Step 5: Read Bronze Delta data ===

df_line_status_bronze = spark.read.format("delta").load(bronze_line_status_path)

display(df_line_status_bronze)

# COMMAND ----------

# === Step 6: Inspect Bronze schema ===

df_line_status_bronze.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformation logic for `line_status`
# MAGIC In the TfL line status payload, one line can contain an array called `lineStatuses`.
# MAGIC
# MAGIC For Silver, we:
# MAGIC - keep one row per line status record
# MAGIC - explode the `lineStatuses` array
# MAGIC - extract key business fields such as:
# MAGIC   - line id
# MAGIC   - line name
# MAGIC   - mode
# MAGIC   - status severity
# MAGIC   - severity description
# MAGIC   - reason
# MAGIC   - created / modified timestamps

# COMMAND ----------

# === Step 7: Flatten lineStatuses ===

from pyspark.sql.functions import explode_outer, col, current_timestamp, to_timestamp

df_line_status_exploded = (
    df_line_status_bronze
    .withColumn("line_status_item", explode_outer(col("lineStatuses")))
)

display(df_line_status_exploded)

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, to_timestamp

df_line_status_silver = (
    df_line_status_exploded
    .select(
        col("id").alias("line_id"),
        col("name").alias("line_name"),
        col("modeName").alias("mode_name"),
        to_timestamp(col("created")).alias("line_created_at"),
        to_timestamp(col("modified")).alias("line_modified_at"),

        col("line_status_item.id").alias("line_status_id"),
        col("line_status_item.lineId").alias("status_line_id"),
        col("line_status_item.statusSeverity").alias("status_severity"),
        col("line_status_item.statusSeverityDescription").alias("status_severity_description"),
        col("line_status_item.reason").alias("status_reason"),
        to_timestamp(col("line_status_item.created")).alias("status_created_at"),

        col("line_status_item.disruption").alias("status_disruption"),
        col("line_status_item.validityPeriods").alias("status_validity_periods"),

        col("source_file_name"),
        col("source_system"),
        col("dataset_name"),
        col("bronze_loaded_at")
    )
    .withColumn("silver_loaded_at", current_timestamp())
)

display(df_line_status_silver)

# COMMAND ----------

# === Step 9: Inspect Silver schema ===

df_line_status_silver.printSchema()

# COMMAND ----------

# === Step 10: Optional quality checks ===

print("Bronze row count:", df_line_status_bronze.count())
print("Silver row count:", df_line_status_silver.count())

# COMMAND ----------

display(
    df_line_status_silver.select(
        "line_id",
        "line_name",
        "mode_name",
        "status_severity",
        "status_severity_description"
    )
)

# COMMAND ----------

print("Starting Silver transformation for dataset: line_status")
print("Bronze path:", bronze_line_status_path)
print("Silver path:", silver_line_status_path)

bronze_count = df_line_status_bronze.count()
print("Bronze input row count:", bronze_count)

silver_count = df_line_status_silver.count()
print("Silver output row count before write:", silver_count)

if silver_count == 0:
    raise ValueError("Silver output has zero rows for line_status")

(
    df_line_status_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(silver_line_status_path)
)

df_line_status_silver_check = spark.read.format("delta").load(silver_line_status_path)
written_count = df_line_status_silver_check.count()

print("Silver row count after write:", written_count)
print("Silver transformation completed successfully for line_status")

display(df_line_status_silver_check.limit(20))

# COMMAND ----------

# === Step 12: Read Silver data back for validation ===

df_line_status_silver_check = spark.read.format("delta").load(silver_line_status_path)

display(df_line_status_silver_check)

# COMMAND ----------

# === Step 13: List Silver files written ===

display(dbutils.fs.ls(silver_line_status_path))

# COMMAND ----------

# Optional: create temp view for quick SQL queries

df_line_status_silver_check.createOrReplaceTempView("vw_line_status_silver")

display(spark.sql("SELECT * FROM vw_line_status_silver LIMIT 10"))

# COMMAND ----------

# MAGIC %md
# MAGIC # Suggested next Silver datasets
# MAGIC
# MAGIC After `line_status`, apply the same pattern to:
# MAGIC
# MAGIC ## disruptions
# MAGIC Flatten:
# MAGIC - category
# MAGIC - categoryDescription
# MAGIC - summary
# MAGIC - description
# MAGIC - created
# MAGIC - lastUpdate
# MAGIC
# MAGIC ## routes
# MAGIC Flatten:
# MAGIC - line id
# MAGIC - line name
# MAGIC - route section fields
# MAGIC - service types
# MAGIC
# MAGIC ## stoppoints
# MAGIC Flatten:
# MAGIC - stop id
# MAGIC - common name
# MAGIC - lat/lon
# MAGIC - modes
# MAGIC - accessibility
# MAGIC - parent/child hierarchy if needed
# MAGIC
# MAGIC ## arrivals
# MAGIC Flatten:
# MAGIC - line id
# MAGIC - vehicle id
# MAGIC - station name
# MAGIC - platform name
# MAGIC - expected arrival
# MAGIC - time to station
# MAGIC - destination

# COMMAND ----------

# MAGIC %md
# MAGIC # Interview explanation
# MAGIC
# MAGIC You can describe this Silver step like this:
# MAGIC
# MAGIC - Bronze stored raw TfL data with minimal transformation
# MAGIC - Silver flattens nested structures and standardizes timestamps
# MAGIC - One row is created per line status record
# MAGIC - Silver keeps the data reusable for Gold marts and dashboarding