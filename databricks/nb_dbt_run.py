# Databricks notebook source
dbutils.secrets.list("kv-tfl-scope")

# COMMAND ----------

import os
import subprocess
import uuid

dbt_project_dir = "/Workspace/Users/rudraprax2012@outlook.com/tfl_project/tfl_project/tfl-analytics-project/dbt"

run_id = str(uuid.uuid4()).replace("-", "")
profiles_dir = f"/local_disk0/tmp/dbt_profiles_{run_id}"
os.makedirs(profiles_dir, exist_ok=True)

dbt_pat = dbutils.secrets.get(scope="kv-tfl-scope", key="databricks-pat")

print("Starting dbt run...")
print("dbt project dir:", dbt_project_dir)
print("profiles dir:", profiles_dir)
print("project dir exists:", os.path.exists(dbt_project_dir))
print("dbt_project.yml exists:", os.path.exists(f"{dbt_project_dir}/dbt_project.yml"))
print("PAT loaded:", len(dbt_pat) > 0)

profiles_yml = f"""
tfl_analytics:
  target: dev
  outputs:
    dev:
      type: databricks
      method: http
      host: adb-7405611658566676.16.azuredatabricks.net
      http_path: /sql/1.0/warehouses/2a4a6a0644117717
      token: {dbt_pat}
      catalog: rudxdatabricks
      schema: default
      threads: 4
"""

profiles_path = f"{profiles_dir}/profiles.yml"

with open(profiles_path, "w") as f:
    f.write(profiles_yml)

print("profiles.yml exists:", os.path.exists(profiles_path))

commands = [
    ["dbt", "debug", "--project-dir", dbt_project_dir, "--profiles-dir", profiles_dir]
]

for cmd in commands:
    print("Running:", " ".join(cmd))
    result = subprocess.run(cmd, capture_output=True, text=True)

    print("STDOUT:")
    print(result.stdout)

    print("STDERR:")
    print(result.stderr)

    print("Return code:", result.returncode)

    if result.returncode != 0:
        raise Exception(
            f"dbt command failed: {' '.join(cmd)}\n\nSTDOUT:\n{result.stdout}\n\nSTDERR:\n{result.stderr}"
        )

print("dbt debug completed successfully.")