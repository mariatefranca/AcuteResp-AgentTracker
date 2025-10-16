# Databricks notebook source
!pip install uv
!uv sync --active --quiet
dbutils.library.restartPython()

# COMMAND ----------

import os
import pandas as pd
import json
import toml
import sys
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

sys.path.append("../../src")
from tools.metrics_calculator import SRAGMetrics
from tools.visual_generator import SRAGVisualization

# COMMAND ----------

# %run ../tools/metric_calculator.py

# COMMAND ----------

# %run ../tools/visual_generator.py

# COMMAND ----------

# Load environment variables.
env_vars = toml.load("../../conf/env_vars.toml")

# Set as environment variables.
for key, value in env_vars.items():
    os.environ[key] = str(value)

# COMMAND ----------

# Agent must provide this to the tool
class AgentNarrativeInput(BaseModel):
    srag_description: str = Field(..., description="A brief description of Severe Acute Respiratory Syndrome (SRAG), with a maximum of 150 words.")
    comment_cases_evolution_count: str = Field(..., description="A commentary of up to 250 words on the evolution of case numbers during the current month and recent months, including both overall trends and differences across states.")
    conclusions_disease_evolution_icu_occupation: str = Field(..., description="A conclusion of up to 300 words discussing recent trends in case numbers, vaccination rates, and ICU occupancy rates, contextualized with relevant current news.")


# COMMAND ----------

class GenerateSRAGReportTool(BaseTool):
    name: str = "generate_srag_html_report"
    description: str = (
        """Generates the final SRAG report by combining metrics, charts, and narratives produced by the agent. Returns the full report in HTML format."""
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
