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

resources = [
    DatabricksServingEndpoint(
        endpoint_name=LLM_ENDPOINT_NAME
    )
]

# agent_model = AGENT() if callable(AGENT) else AGENT
input_example = {"messages": [{"role": "user", "content": "O que é SRAG?"}]}
dependencies = toml.load("../../pyproject.toml")["project"]["dependencies"]

with mlflow.start_run():
    model_info = mlflow.pyfunc.log_model(
        artifact_path="agent",             # folder name inside MLflow run
        python_model="agent.py",          # class instance, not string
        pip_requirements=dependencies,     # loaded from pyproject.toml
        input_example=input_example,
        resources=resources,
        registered_model_name="srag_model" # MLflow Registry name
    )

# COMMAND ----------

# resources = [
#     DatabricksServingEndpoint(
#         endpoint_name=LLM_ENDPOINT_NAME
#     )
# ]

# input_example = {"messages": [{"role": "user", "content": "O que é SRAG?"}]}
# dependencies = toml.load("../../pyproject.toml")["project"]["dependencies"]

# with mlflow.start_run():
#     model_info = mlflow.pyfunc.log_model(
#         "agent",
#         python_model="agent.py",
#         pip_requirements=dependencies,
#         input_example = input_example,
#         resources=resources,
#         # registered_model_name="srag_model",
#     )
