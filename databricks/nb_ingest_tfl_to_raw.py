# Databricks notebook source
import json
import requests
from datetime import datetime
from pathlib import Path

storage_account_name = "sttflanalyticsdevrudx"
scope_name = "kv-tfl-scope"
storage_key_secret = "adls-storage-key"
tfl_key_secret = "tflappkey"

base_url = "https://api.tfl.gov.uk"

# COMMAND ----------

storage_account_key = dbutils.secrets.get(
    scope=scope_name,
    key=storage_key_secret
)

tfl_app_key = dbutils.secrets.get(
    scope=scope_name,
    key=tfl_key_secret
)

print("Secrets loaded successfully.")

# COMMAND ----------

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key
)

print("ADLS Spark configuration applied.")

# COMMAND ----------

def get_run_timestamp():
    return datetime.utcnow().strftime("%Y%m%d_%H%M%S")

def get_partition_path():
    now = datetime.utcnow()
    return f"year={now.year}/month={now.month:02d}/day={now.day:02d}"

# COMMAND ----------

def call_tfl_api(endpoint: str, params: dict = None):
    if params is None:
        params = {}

    params["app_key"] = tfl_app_key

    url = f"{base_url}{endpoint}"
    response = requests.get(url, params=params, timeout=30)

    print("Calling:", response.url)
    print("Status code:", response.status_code)

    if response.status_code != 200:
        raise ValueError(f"API call failed for {endpoint}. Status: {response.status_code}. Response: {response.text[:500]}")

    return response.json()

# COMMAND ----------

def write_raw_json_to_adls(dataset_name: str, file_prefix: str, data):
    timestamp = get_run_timestamp()
    partition_path = get_partition_path()

    raw_output_path = (
        f"abfss://raw@{storage_account_name}.dfs.core.windows.net/"
        f"{dataset_name}/{partition_path}/{file_prefix}_{timestamp}.json"
    )

    json_string = json.dumps(data, indent=2)

    dbutils.fs.put(raw_output_path, json_string, overwrite=True)

    print(f"Written raw file: {raw_output_path}")
    return raw_output_path

# COMMAND ----------

def ingest_line_status():
    print("Starting ingestion for line_status")
    data = call_tfl_api("/Line/Mode/tube/Status")
    path = write_raw_json_to_adls("line_status", "tube_line_status", data)
    print("Completed ingestion for line_status")
    return path

def ingest_disruptions():
    print("Starting ingestion for disruptions")
    data = call_tfl_api("/Line/Mode/tube/Disruption")
    path = write_raw_json_to_adls("disruptions", "tube_disruptions", data)
    print("Completed ingestion for disruptions")
    return path

def ingest_routes():
    print("Starting ingestion for routes")
    data = call_tfl_api("/Line/Mode/tube/Route")
    path = write_raw_json_to_adls("routes", "tube_routes", data)
    print("Completed ingestion for routes")
    return path

def ingest_stoppoints():
    print("Starting ingestion for stoppoints")
    data = call_tfl_api("/StopPoint/Mode/tube")
    path = write_raw_json_to_adls("stoppoints", "tube_stoppoints", data)
    print("Completed ingestion for stoppoints")
    return path

def ingest_arrivals():
    print("Starting ingestion for arrivals")
    data = call_tfl_api("/StopPoint/940GZZLUVIC/Arrivals")
    path = write_raw_json_to_adls("arrivals", "victoria_arrivals", data)
    print("Completed ingestion for arrivals")
    return path

# COMMAND ----------

print("Starting TfL raw ingestion notebook run...")

written_files = []

written_files.append(ingest_line_status())
written_files.append(ingest_disruptions())
written_files.append(ingest_routes())
written_files.append(ingest_stoppoints())
written_files.append(ingest_arrivals())

print("All ingestion steps completed.")
print("Files written:")

for file_path in written_files:
    print(file_path)

# COMMAND ----------

display(dbutils.fs.ls(f"abfss://raw@{storage_account_name}.dfs.core.windows.net/"))

# COMMAND ----------

display(dbutils.fs.ls(f"abfss://raw@{storage_account_name}.dfs.core.windows.net/line_status/"))

# COMMAND ----------

from datetime import datetime
print("Ingestion notebook finished successfully at:", datetime.utcnow())

# COMMAND ----------

