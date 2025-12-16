# Databricks notebook source
# Install dependencies
!pip install uv
!uv sync --active --quiet
!pip install --upgrade databricks-cli>=0.205
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %pip install --upgrade databricks-sdk
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import os
import subprocess

# COMMAND ----------

!curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh

# COMMAND ----------

! cd /root/bin/databricks bundle validate -t dev

# COMMAND ----------

ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
repo_root = ctx.notebookPath().get().replace("main", "")
bundle_path = f"/Workspace/{repo_root}"

os.environ["DATABRICKS_HOST"] = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()
os.environ["DATABRICKS_TOKEN"] = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

# COMMAND ----------

!pip list

# COMMAND ----------

!databricks bundle validate -t dev

# COMMAND ----------

subprocess.run(
    ["databricks", "bundle", "validate", "-t", "dev"],
    cwd=bundle_path,
    check=True
)

# COMMAND ----------

subprocess.run(
    ["databricks", "bundle", "deploy", "-t", "dev"],
    cwd=bundle_path,
    check=True
)

subprocess.run(
    ["databricks", "bundle", "run", "daily_report_job", "-t", "dev"],
    cwd=bundle_path,
    check=True
)

