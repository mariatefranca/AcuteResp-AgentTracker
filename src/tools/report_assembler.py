# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

# !pip install uv
# !uv sync --active --quiet
# dbutils.library.restartPython()

# COMMAND ----------

import sys
sys.path.append("../../src")

# COMMAND ----------

# MAGIC %run ../tools/metric_calculator.py

# COMMAND ----------

# MAGIC %run ../tools/visual_generator.py

# COMMAND ----------


import os
import pandas as pd
import json
import toml
from typing import Optional
from pyspark.sql.connect.dataframe import DataFrame
import pyspark.sql.functions as F
import json
import requests
import pyspark.sql.types as T
from datetime import timedelta, date
from pydantic import BaseModel, Field
import os
from langchain.tools import BaseTool
from typing import Type
from jinja2 import Template
from datetime import timedelta, date

# COMMAND ----------

# Load environment variables.
env_vars = toml.load("../../conf/env_vars.toml")

# Set as environment variables.
for key, value in env_vars.items():
    os.environ[key] = str(value)

srag_df = spark.read.table(F'{env_vars["CATALOG"]}.{env_vars["FS_SCHEMA"]}.srag_features')
hospital_df = spark.read.table(F'{env_vars["CATALOG"]}.{env_vars["FS_SCHEMA"]}.hospital_features')

# COMMAND ----------

# Agent must provide this to the tool
class AgentNarrativeInput(BaseModel):
    srag_description: str = Field(..., description="Breve descrição de SRAG em até 100 palavras.")
    comment_cases_evolution_count: str = Field(..., description="Comentário sobre evolução do número de casos nos últimos meses.")
    conclusions_disease_evolution_icu_occupation: str = Field(..., description="Conclusões sobre evolução da doença e ocupação das UTIs.")


# COMMAND ----------

class GenerateSRAGReportTool(BaseTool):
    name: str = "generate_srag_html_report"
    description: str = (
        "Gera o relatório final de SRAG combinando métricas, gráficos e textos "
        "produzidos pelo agente. Retorna HTML completo do relatório."
    )
    # metric_calculator: type = SRAGMetrics
    # visual_generator: type = SRAGVisualization
    args_schema: Type[BaseModel] = AgentNarrativeInput

    # def __init__(self, metric_calculator: SRAGMetrics, visual_generator: SRAGVisualization):
    #     super().__init__()
    #     self.metric_calculator = metric_calculator
    #     self.visual_generator = visual_generator

    def _load_template(self, template_path: str) -> Template:
        with open(template_path, "r") as f:
            return Template(f.read())

    def _run(self, srag_description: str, comment_cases_evolution_count: str,
             conclusions_disease_evolution_icu_occupation: str) -> str:

        # Load Template      
        template_path = "../utils/srag_report_template.html"
        template = self._load_template(template_path)

        # Gather Metrics & Visuals 
        metric_calculator = SRAGMetrics()
        visual_generator = SRAGVisualization()
             
        metrics = metric_calculator.generate_report_metrics()
        visuals = visual_generator.generate_report_plots()

        # Merge Agent Narrative      
        agent_context = {
            "srag_description": srag_description,
            "comment_cases_evolution_count": comment_cases_evolution_count,
            "conclusions_disease_evolution_icu_occupation": conclusions_disease_evolution_icu_occupation,
        }

        # Final Merge Context for Rendering      
        context = {}
        context.update(metrics)
        context.update(visuals)
        context.update(agent_context)

        # Render Final HTML      
        final_report_html = template.render(**context)
        report_name = f"../../reports/SRAG_report_{date.today().strftime('%Y-%m-%d')}.html"
        with open(report_name, "w", encoding="utf-8") as f:
            f.write(final_report_html)
        return displayHTML(final_report_html)
