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

# Type your mesage to the AI agent by replacing "Digite a sua mensagem aqui".
user_message = {"messages": [{
    "role": "user", 
    "content": 
        """
        Digite a sua mensagem aqui.
        """
        }]}

# Run the question to the ai agent.
ai_model.predict(user_message
    )
