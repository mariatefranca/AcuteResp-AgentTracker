# Databricks notebook source
!pip install uv
!uv sync --active --quiet
dbutils.library.restartPython()

# COMMAND ----------

import mlflow
import toml

# COMMAND ----------

env_vars = toml.load("../../conf/env_vars.toml")

# COMMAND ----------

# Configure UC model location
UC_MODEL_NAME = f"{env_vars['CATALOG']}.{env_vars['FS_SCHEMA']}.srag_model"

# COMMAND ----------

# Load the registered model
ai_model = mlflow.pyfunc.load_model(
    f"models:/{UC_MODEL_NAME}@champion"
)

# COMMAND ----------

ai_model.predict(
    {"messages": [{"role": "user", "content": 
        "Gere o relatório de casos de SRAG de hoje."}]}
    )
