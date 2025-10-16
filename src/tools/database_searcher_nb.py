# Databricks notebook source
# MAGIC %md
# MAGIC https://python.langchain.com/v0.2/docs/integrations/tools/spark_sql/

# COMMAND ----------

!pip install uv
!uv add databricks-sql-connector databricks-sqlalchemy --active --quiet
!uv sync --active --quiet
dbutils.library.restartPython()

# COMMAND ----------

import io
import os
import sys
import toml
from dotenv import load_dotenv
from langchain.memory import ConversationBufferMemory
from langchain_community.agent_toolkits import SparkSQLToolkit, create_spark_sql_agent
from langchain_community.utilities.spark_sql import SparkSQL
from langchain_openai import ChatOpenAI
from langchain.tools import BaseTool
from pydantic import BaseModel
from pyspark.sql import SparkSession
from typing import Optional, Type, Any, Dict



# COMMAND ----------

# Load environment variables.
env_vars = toml.load("../../conf/env_vars.toml")

# Set as environment variables.
for key, value in env_vars.items():
    os.environ[key] = str(value)

# COMMAND ----------

# Load credentials variables from .env file.
load_dotenv("../../.env")

# COMMAND ----------

system_prompt = """'You are an agent designed to interact with Spark SQL.
Given an input question, create a syntactically correct Spark SQL query to run, then look at the results of the query and return the answer.You have access to a table srag_features with SRAG disease statistics and to a table srag_features_dictionary, which contais a column named 'coluna' with all the srag_features names and a column 'descricao' with all column's descriptions.
Unless the user specifies a specific number of examples they wish to obtain, always limit your query to at most 5 results.You can order the results by a relevant column to return the most interesting examples in the database. Never query for all the columns from a specific table, only ask for the relevant columns given the question.
You have access to tools for interacting with the database. Only use the below tools. Only use the information returned by the below tools to construct your final answer.\nYou MUST double check your query before executing it. If you get an error while executing a query, rewrite the query and try again.
DO NOT make any DML statements (INSERT, UPDATE, DELETE, DROP etc.) to the database.
If the question does not seem related to the database, just return 'I don't know' as the answer, if there is no data available for calculating a emtric or answering the question, you can return 'There is no data available in the database to answer the current query.'"""

# COMMAND ----------

class SparkSQLQuerySchema(BaseModel):
    query: str
    max_results: Optional[int] = 100

class SparkSQLQueryTool(BaseTool):
    name: str = "spark_sql_query_tool"
    description: str = (
        """A specialized agent designed to search and query structured databases to retrieve relevant information and insights from a SRAG dataset, using SparkSQLToolkit. It interprets natural language or structured prompts, translates them into READ optimized SQL or Spark SQL queries, executes them safely, and returns concise answers.
        This agent is ideal for tasks involving:
        - Data exploration and lookup across datasets.
        - Data analysis and statistical calculations.
        - Retrieving specific records or entities.
        The agent returns results and saves verbose logs in memory."""
    )
    memory: Optional[ConversationBufferMemory] = None
    llm: Optional[ChatOpenAI] = None
    toolkit: Optional[SparkSQLToolkit] = None
    agent_executor: Any = None
    args_schema: Type[BaseModel] = SparkSQLQuerySchema

    def __init__(self, memory: Optional[ConversationBufferMemory] = None, **kwargs):
        super().__init__(memory=memory, **kwargs)

        # Initialize Spark
        spark = SparkSession.builder.getOrCreate()
        catalog = os.environ.get("CATALOG")
        schema = os.environ.get("FS_SCHEMA")
        spark.sql(f"USE CATALOG {catalog}")
        spark.sql(f"USE SCHEMA {schema}")

        # Memory for verbose logs
        self.memory = memory or ConversationBufferMemory(
            memory_key="chat_history",
            return_messages=True
        )

        # LLM
        OPENAI_API_KEY = os.getenv("OPEN_AI_KEY")
        llm = ChatOpenAI(api_key=OPENAI_API_KEY, temperature=0, prefix=self._prefix)

        # Agent setup
        spark_sql = SparkSQL(catalog=catalog, schema=schema)
        self.toolkit = SparkSQLToolkit(db=spark_sql, llm=llm)
        self.agent_executor = create_spark_sql_agent(
            llm=llm,
            toolkit=self.toolkit,
            memory=self.memory,
            verbose=True,
            agent_executor_kwargs={"memory": memory, 'handle_parsing_errors': True},
        )
    def _prefix(self) -> str:
        system_prompt = """'You are an agent designed to interact with Spark SQL.
        Given an input question, create a syntactically correct Spark SQL query to run, then look at the results of the query and return the answer.You have access to a table srag_features with SRAG disease statistics and to a table srag_features_dictionary, which contais a column named 'coluna' with all the srag_features names and a column 'descricao' with all column's descriptions.
        Unless the user specifies a specific number of examples they wish to obtain, always limit your query to at most 5 results.You can order the results by a relevant column to return the most interesting examples in the database. Never query for all the columns from a specific table, only ask for the relevant columns given the question.
        You have access to tools for interacting with the database. Only use the below tools. Only use the information returned by the below tools to construct your final answer.\nYou MUST double check your query before executing it. If you get an error while executing a query, rewrite the query and try again.
        DO NOT make any DML statements (INSERT, UPDATE, DELETE, DROP etc.) to the database.
        If the question does not seem related to the database, just return 'I don't know' as the answer, if there is no data available for calculating a metric or answering a question, you can return 'There is no data available in the database to answer the current query.'"""
        return system_prompt

    def _run(self, query: str, max_results: int = 100) -> Dict[str, Any]:
        """
        Run the SQL query using the SparkSQL agent executor.
        Returns a dict with:
            query: The query used as input to the agent executor.
            results: The final answer provided by the agent executor
            verbose: verbose logs from memory.
        """
        buffer = io.StringIO()
        stdout = sys.stdout
        sys.stdout = buffer  # Redirect print output

        try:
            result = self.agent_executor.run(query)
        except Exception as e:
            result = []
            self.memory.save_context({"input": query}, {"output": str(e)})
        finally:
            sys.stdout = stdout

        # Store captured verbose text in memory for other agents.
        verbose_text = buffer.getvalue()
        self.memory.save_context(
            {"query": query},
            {"verbose_log": verbose_text[:5000]}  # Truncate if large
        )

        # Limit result size
        if isinstance(result, list):
            result = result[:max_results]

        return {
            "query": query,
            "results": result,
            "verbose": verbose_text
        }
