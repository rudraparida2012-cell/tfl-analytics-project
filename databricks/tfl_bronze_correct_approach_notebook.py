# Databricks notebook source
# MAGIC %md
# MAGIC # TfL Bronze Layer in Databricks — Correct Approach
# MAGIC
# MAGIC This notebook uses the **correct production-style Bronze approach** for your current Databricks setup:
# MAGIC
# MAGIC - **Databricks secret scope** for ADLS credentials
# MAGIC - **Unity Catalog-compatible metadata** using `_metadata.file_path`
# MAGIC - **Path-based Delta validation** instead of table registration
# MAGIC - Avoids the Unity Catalog external location issue for now
# MAGIC
# MAGIC ## What this notebook does
# MAGIC 1. Reads ADLS storage key from Databricks secret scope  
# MAGIC 2. Connects to the `raw` and `bronze` containers  
# MAGIC 3. Reads raw JSON from ADLS  
# MAGIC 4. Adds Bronze metadata columns  
# MAGIC 5. Writes Delta files into the Bronze container  
# MAGIC 6. Reads the Bronze Delta files back for validation  
# MAGIC
# MAGIC > This notebook intentionally **does not register an external table**, because your workspace requires an **external location** for Unity Catalog table registration.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Update these values before running
# MAGIC - `storage_account_name`
# MAGIC - `scope_name`
# MAGIC - `secret_key_name`
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Raw JSON files already uploaded to ADLS `raw` container
# MAGIC - Secret scope already created in Databricks
# MAGIC - ADLS storage key stored in the secret scope

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

# === Step 3: Configure Spark to access ADLS ===

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key
)

print("Spark ADLS configuration applied.")

# COMMAND ----------

# === Step 4: Verify access to raw and bronze containers ===

display(dbutils.fs.ls(f"abfss://raw@{storage_account_name}.dfs.core.windows.net/"))

# COMMAND ----------

display(dbutils.fs.ls(f"abfss://bronze@{storage_account_name}.dfs.core.windows.net/"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze helper function
# MAGIC This function:
# MAGIC - reads raw JSON from a dataset folder
# MAGIC - adds Bronze metadata columns
# MAGIC - writes Delta files to the Bronze container
# MAGIC - reads the Delta files back for validation

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit, col

def build_bronze_dataset(dataset_name: str):
    raw_path = f"abfss://raw@{storage_account_name}.dfs.core.windows.net/{dataset_name}/"
    bronze_path = f"abfss://bronze@{storage_account_name}.dfs.core.windows.net/tfl/{dataset_name}/"

    print(f"Reading raw dataset from: {raw_path}")

    df_raw = (
        spark.read
        .option("multiline", "true")
        .json(raw_path)
    )

    print("Raw schema:")
    df_raw.printSchema()

    df_bronze = (
        df_raw
        .withColumn("bronze_loaded_at", current_timestamp())
        .withColumn("source_file_name", col("_metadata.file_path"))
        .withColumn("source_system", lit("tfl_api"))
        .withColumn("dataset_name", lit(dataset_name))
    )

    print(f"Writing Bronze Delta files to: {bronze_path}")

    (
    df_bronze.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(bronze_path)
)

    print("Write complete. Reading Delta files back for validation...")

    df_check = spark.read.format("delta").load(bronze_path)
    return df_raw, df_bronze, df_check, raw_path, bronze_path

# COMMAND ----------

# MAGIC %md
# MAGIC # Dataset 1 — line_status
# MAGIC Run this first. Once it works, repeat for the other datasets.

# COMMAND ----------

# === Step 5: Build Bronze for line_status ===

df_line_status_raw, df_line_status_bronze, df_line_status_check, raw_line_status_path, bronze_line_status_path = build_bronze_dataset("line_status")

# COMMAND ----------

# Preview raw data
display(df_line_status_raw)

# COMMAND ----------

# Preview Bronze dataframe
display(df_line_status_bronze)

# COMMAND ----------

# Validate Bronze Delta files
display(df_line_status_check)

# COMMAND ----------

# List Bronze files written
display(dbutils.fs.ls(bronze_line_status_path))

# COMMAND ----------

# MAGIC %md
# MAGIC # Optional — Repeat for other datasets
# MAGIC
# MAGIC Uncomment and run each block one by one after `line_status` works.

# COMMAND ----------

# line_status
print("Starting Bronze load for dataset: line_status")

df_line_status_raw, df_line_status_bronze, df_line_status_check, raw_line_status_path, bronze_line_status_path = build_bronze_dataset("line_status")

print("Raw path:", raw_line_status_path)
print("Bronze path:", bronze_line_status_path)

raw_count = df_line_status_raw.count()
print("Raw row count:", raw_count)

bronze_count = df_line_status_bronze.count()
print("Bronze row count before write/readback validation:", bronze_count)

if bronze_count == 0:
    raise ValueError("Bronze output has zero rows for line_status")

written_count = df_line_status_check.count()
print("Bronze row count after write:", written_count)

print("Bronze load completed successfully for line_status")

display(df_line_status_check.limit(20))

# COMMAND ----------

# disruptions
print("Starting Bronze load for dataset: disruptions")

df_disruptions_raw, df_disruptions_bronze, df_disruptions_check, raw_disruptions_path, bronze_disruptions_path = build_bronze_dataset("disruptions")

print("Raw path:", raw_disruptions_path)
print("Bronze path:", bronze_disruptions_path)

raw_count = df_disruptions_raw.count()
print("Raw row count:", raw_count)

bronze_count = df_disruptions_bronze.count()
print("Bronze row count before write/readback validation:", bronze_count)

if bronze_count == 0:
    raise ValueError("Bronze output has zero rows for disruptions")

written_count = df_disruptions_check.count()
print("Bronze row count after write:", written_count)

print("Bronze load completed successfully for disruptions")

display(df_disruptions_check.limit(20))

# COMMAND ----------

# routes
print("Starting Bronze load for dataset: routes")

df_routes_raw, df_routes_bronze, df_routes_check, raw_routes_path, bronze_routes_path = build_bronze_dataset("routes")

print("Raw path:", raw_routes_path)
print("Bronze path:", bronze_routes_path)

raw_count = df_routes_raw.count()
print("Raw row count:", raw_count)

bronze_count = df_routes_bronze.count()
print("Bronze row count before write/readback validation:", bronze_count)

if bronze_count == 0:
    raise ValueError("Bronze output has zero rows for routes")

written_count = df_routes_check.count()
print("Bronze row count after write:", written_count)

print("Bronze load completed successfully for routes")

display(df_routes_check.limit(20))

# COMMAND ----------

# stoppoints
print("Starting Bronze load for dataset: stoppoints")

df_stoppoints_raw, df_stoppoints_bronze, df_stoppoints_check, raw_stoppoints_path, bronze_stoppoints_path = build_bronze_dataset("stoppoints")

print("Raw path:", raw_stoppoints_path)
print("Bronze path:", bronze_stoppoints_path)

raw_count = df_stoppoints_raw.count()
print("Raw row count:", raw_count)

bronze_count = df_stoppoints_bronze.count()
print("Bronze row count before write/readback validation:", bronze_count)

if bronze_count == 0:
    raise ValueError("Bronze output has zero rows for stoppoints")

written_count = df_stoppoints_check.count()
print("Bronze row count after write:", written_count)

print("Bronze load completed successfully for stoppoints")

df_stoppoints_check.printSchema()


# COMMAND ----------


# arrivals
print("Starting Bronze load for dataset: arrivals")

df_arrivals_raw, df_arrivals_bronze, df_arrivals_check, raw_arrivals_path, bronze_arrivals_path = build_bronze_dataset("arrivals")

print("Raw path:", raw_arrivals_path)
print("Bronze path:", bronze_arrivals_path)

raw_count = df_arrivals_raw.count()
print("Raw row count:", raw_count)

bronze_count = df_arrivals_bronze.count()
print("Bronze row count before write/readback validation:", bronze_count)

if bronze_count == 0:
    raise ValueError("Bronze output has zero rows for arrivals")

written_count = df_arrivals_check.count()
print("Bronze row count after write:", written_count)

print("Bronze load completed successfully for arrivals")

display(df_arrivals_check.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC # Notes for interviews
# MAGIC
# MAGIC You can explain the final Bronze approach like this:
# MAGIC
# MAGIC - Raw TfL JSON files are landed in ADLS `raw`
# MAGIC - Databricks reads the raw files securely using a secret scope
# MAGIC - Bronze metadata columns are added
# MAGIC - Data is stored as Delta files in the `bronze` container
# MAGIC - Unity Catalog table registration was deferred because the workspace requires an external location for path-based external table registration
# MAGIC
# MAGIC This is still a valid production-style Bronze implementation.

# COMMAND ----------

# MAGIC %md
# MAGIC # Next step
# MAGIC After Bronze validation is complete, move to **Silver Layer**:
# MAGIC - flatten nested fields
# MAGIC - standardize timestamps
# MAGIC - deduplicate records
# MAGIC - create cleaner analytical entities