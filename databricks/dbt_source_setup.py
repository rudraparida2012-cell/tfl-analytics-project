# Databricks notebook source
spark.sql("SHOW TABLES IN rudxdatabricks.default").show(truncate=False)
spark.sql("DROP VIEW IF EXISTS rudxdatabricks.default.line_status")
df_line_status_silver.write.mode("overwrite").saveAsTable("rudxdatabricks.default.line_status")
spark.sql("SHOW TABLES IN rudxdatabricks.default").show(truncate=False)
display(spark.sql("SELECT * FROM rudxdatabricks.default.line_status LIMIT 10"))

# COMMAND ----------

storage_account_name = "sttflanalyticsdevrudx"

storage_account_key = dbutils.secrets.get(
    scope="kv-tfl-scope",
    key="adls-storage-key"
)

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key
)


# COMMAND ----------

silver_line_status_path = f"abfss://silver@{storage_account_name}.dfs.core.windows.net/tfl/line_status/"
silver_line_status_path

# COMMAND ----------

df_line_status_silver = spark.read.format("delta").load(silver_line_status_path)
display(df_line_status_silver)

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS rudxdatabricks.default")

# COMMAND ----------

df_line_status_silver.write.mode("overwrite").saveAsTable("rudxdatabricks.default.line_status")

# COMMAND ----------

spark.sql("SHOW TABLES IN rudxdatabricks.default").show(truncate=False)

# COMMAND ----------

display(spark.sql("SELECT * FROM rudxdatabricks.default.line_status LIMIT 10"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM rudxdatabricks.default.dim_line LIMIT 10;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM rudxdatabricks.default.fact_line_status LIMIT 10;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM rudxdatabricks.default.kpi_line_status_summary LIMIT 10;

# COMMAND ----------

