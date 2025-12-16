# Databricks notebook source
!pip install uv --quiet
!uv sync --active --quiet
dbutils.library.restartPython()

# COMMAND ----------

import os
import mlflow
import sys
import toml
import pyspark.sql.functions as F
from databricks_langchain import ChatDatabricks
import datetime
from typing import Any, Generator, Optional, Sequence, Union
from databricks_langchain import ChatDatabricks
from langchain_core.tools import BaseTool, tool
from langchain_core.runnables import RunnableConfig, RunnableLambda
from langchain_core.language_models import LanguageModelLike
from langgraph.graph import END, StateGraph
from langgraph.graph.state import CompiledStateGraph
from langgraph.prebuilt.tool_node import ToolNode
from mlflow.langchain.chat_agent_langgraph import ChatAgentState, ChatAgentToolNode
from mlflow.pyfunc import ChatAgent
from mlflow.types.agent import ChatAgentMessage, ChatAgentResponse, ChatAgentChunk, ChatContext
from mlflow.types.llm import ChatCompletionResponse, ChatChoice, ChatMessage, ChatCompletionChunk, ChatChunkChoice, ChatChoiceDelta
from langchain_core.tools import StructuredTool
from langgraph.graph.message import add_messages
from langchain_core.messages import (
    AIMessage,
    AIMessageChunk,
    BaseMessage,
    convert_to_openai_messages,
)
from pydantic import BaseModel, create_model
from typing import Annotated, TypedDict
from mlflow.models.resources import DatabricksFunction, DatabricksServingEndpoint

mlflow.set_registry_uri("databricks-uc")

# COMMAND ----------

sys.path.append("../../src")

from agents.agent import AGENT

# COMMAND ----------

env_vars = toml.load("../../conf/env_vars.toml")

# Set as environment variables.
for key, value in env_vars.items():
    os.environ[key] = str(value)

# COMMAND ----------

LLM_ENDPOINT_NAME = env_vars["LLM_ENDPOINT_NAME"]

# COMMAND ----------

# MAGIC %md
# MAGIC # Log Agent Model

# COMMAND ----------

from databricks.sdk import WorkspaceClient
client = WorkspaceClient().serving_endpoints.get_open_ai_client()

# COMMAND ----------

# Configure UC model location
UC_MODEL_NAME = f"{env_vars['CATALOG']}.{env_vars['FS_SCHEMA']}.srag_model"

# COMMAND ----------

!pip list

# COMMAND ----------

dependencies = toml.load("../../pyproject.toml")["project"]["dependencies"]
dependencies

# COMMAND ----------

resources = [
    DatabricksServingEndpoint(
        endpoint_name=LLM_ENDPOINT_NAME
    )
]

input_example = {"messages": [{"role": "user", "content": "Dê me o relatório de casos de SRAG de hoje?"}]}
dependencies = toml.load("../../pyproject.toml")["project"]["dependencies"]

with mlflow.start_run():
    model_info = mlflow.pyfunc.log_model(
        artifact_path="agent",             # folder name inside MLflow run
        python_model="agent.py",          # class instance, not string
        pip_requirements=dependencies,     # loaded from pyproject.toml
        input_example=input_example,
        resources=resources,
       code_paths=["../../src", "../tools", "../agent_config", "../utils"],
       artifacts={
            "env_vars": "../../conf/env_vars.toml"
        },
        # registered_model_name=f'{env_vars["CATALOG"]}.{env_vars["FS_SCHEMA"]}.srag_model' # MLflow Registry name
    )

# COMMAND ----------

model_info.name

# COMMAND ----------

# import mlflow.deployments
from databricks import agents

# client = mlflow.deployments.get_deploy_client("databricks")


# COMMAND ----------

agents.list_deployments()

# COMMAND ----------

agents.delete_deployment(model_name='DEV.SRAG_FEATURE_STORE.srag_model')

# COMMAND ----------

# Configure UC model location
UC_MODEL_NAME = f"{env_vars['CATALOG']}.{env_vars['FS_SCHEMA']}.srag_model"

# Register to Unity Catalog
registered_model = mlflow.register_model(
  model_uri=model_info.model_uri, name=UC_MODEL_NAME
)

# COMMAND ----------

from langchain.memory import ConversationBufferMemory


# COMMAND ----------

# Deploy to enable the review app and create an API endpoint
deployment_info = agents.deploy(
  model_name=UC_MODEL_NAME, model_version=registered_model.version, scale_to_zero=True, deploy_feedback_model=False
)

# COMMAND ----------

# from mlflow import MlflowClient

# client = MlflowClient()

# models = [
#     m.name for m in client.search_registered_models()
# ]

# COMMAND ----------

# model = mlflow.pyfunc.load_model(model_info.model_uri)
# AGENT.predict({
#     "messages": [{"role": "user", "content": "O que é SRAG?"}]
# })
