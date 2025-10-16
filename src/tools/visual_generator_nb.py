# Databricks notebook source
# !pip install uv
# !uv sync --active --quiet
# dbutils.library.restartPython()

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
import plotly.express as px
import pyspark.sql.types as T
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import timedelta, date
from pydantic import BaseModel, Field
from datetime import date

from metric_calculator import SRAGMetrics

# COMMAND ----------

# MAGIC %run ../tools/metric_calculator.py

# COMMAND ----------

# Load environment variables.
env_vars = toml.load("../../conf/env_vars.toml")

# Set as environment variables.
for key, value in env_vars.items():
    os.environ[key] = str(value)

# COMMAND ----------

class SRAGVisualization:
    def __init__(
        self, 
        srag_df: Optional[DataFrame] = None, 
        hospital_df: Optional[DataFrame] = None,
        metric_calculator: Optional["SRAGMetrics"] = None) -> None:
        """
        srag_df: optional main dataset (e.g., monthly cases)
        hospital_df: optional secondary dataset (e.g., hospitalizations)
        """
        catalog = os.environ["CATALOG"]
        schema = os.environ["FS_SCHEMA"]
        self.srag_df = srag_df if srag_df is not None else spark.read.table(f'{catalog}.{schema}.srag_features')

        self.hospital_df = hospital_df if hospital_df is not None else spark.read.table(f'{catalog}.{schema}.hospital_features')

        self.metric_calculator = metric_calculator if metric_calculator is not None else SRAGMetrics()

    # Visualization functions
    def _plot_cases_per_month(
        self, 
        cases_pd: Optional[pd.DataFrame] = None, 
        title: Optional[str] = "Número de casos por mês"
        ) -> None:
        """
        Plots a single time series of cases per month.
        
        Args:
            cases_pd (DataFrame): Pandas DataFrame with ['year_month', 'count']. If None, defaults to the last 12 month.
            title (str) defaults, 
            title (str): Plot title.
        """
        # Calculate cases per month of last 12 months if not provided.
        if cases_pd is None:
            cases_pd = self.metric_calculator.calculate_cases_per_month()

        # Ensure correct datetime type.
        cases_pd["year_month"] = pd.to_datetime(cases_pd["year_month"])
        
        # Plot a time series of cases per month.
        fig = px.line(
            cases_pd,
            x="year_month",
            y="count",
            # title=title,
            markers=True,
            labels={"year_month": "Mês", "count": "Número de casos"}
        )
        
        fig.update_layout(
            xaxis_title="Mês",
            yaxis_title="Número de casos",
            xaxis=dict(dtick="M1", tickformat="%b\n%Y"),
            template="plotly_white"
        )
        return (fig.to_html(include_plotlyjs='cdn', full_html=False), cases_pd)

    
    def _plot_cases_per_day(
        self, 
        cases_per_day_pd: Optional[pd.DataFrame] = None, 
        title: Optional[str] = "Número de casos por dia dos últimos 30 dias"
        ) -> None:
        """
        Plots a single time series of cases per day of last 30 days.
        
        Args:
            cases_per_day_pd (DataFrame): Pandas DataFrame with ['DT_NOTIFIC', 'count'].
            title (str): Plot title.
        """
        if cases_per_day_pd is None:
            cases_per_day_pd = self.metric_calculator.calculate_cases_per_day(self.srag_df)
        
        # Ensure correct datetime type.
        cases_per_day_pd["DT_NOTIFIC"] = pd.to_datetime(cases_per_day_pd["DT_NOTIFIC"])
        last_date = cases_per_day_pd["DT_NOTIFIC"].max()
        first_date = cases_per_day_pd["DT_NOTIFIC"].min()
        end_30_days_interval = first_date + timedelta(days=30)
        # Create full date range.
        full_range = pd.date_range(start=first_date, end=end_30_days_interval, freq="D")
        
        # Reindex to include missing days with 0
        cases_per_day_pd = (
            cases_per_day_pd.set_index("DT_NOTIFIC")
            .reindex(full_range, fill_value=0)
            .rename_axis("DT_NOTIFIC")
            .reset_index()
        )
        
        fig = px.line(
            cases_per_day_pd,
            x="DT_NOTIFIC",
            y="count",
            # title=title,
            markers=True,
            labels={"DT_NOTIFIC": "Dia", "count": "Número de casos"}
        )
        
        fig.update_layout(
            xaxis_title="Dia",
            yaxis_title="Número de casos",
            xaxis=dict(
            dtick="D1",
            tickformat="%d\n%b",
            range=[first_date, last_date]
            ),
            template="plotly_white"
        )

        if end_30_days_interval > last_date:
            fig.add_annotation(
                x=(end_30_days_interval - timedelta(days=8)),
                y=cases_per_day_pd["count"].max(),
                text=f"Último dado disponível: {last_date.strftime('%d/%m/%Y')}",
                showarrow=False,
                bgcolor="white"
            )

        return (fig.to_html(include_plotlyjs='cdn', full_html=False), cases_per_day_pd)

    def _plot_cases_by_uf(self, srag_df: Optional[pd.DataFrame] = None):
        """Plots a choropleth map of SRAG cases per 100,000 inhabitants by Brazilian state (UF)
        using self.srag_df and 2022 IBGE population data."""
        
        if srag_df is None:
            srag_df = self.srag_df

        # Calculate count of cases per UF.
        cases_per_uf = (
            srag_df
            .dropna(subset=["SG_UF_NOT"])
            .groupBy("SG_UF_NOT")
            .count()
            .orderBy(F.desc("count"))
        )
        cases_pd = cases_per_uf.toPandas()

        # Upload GeoJSON of Brazil states.
        url = "https://raw.githubusercontent.com/codeforamerica/click_that_hood/master/public/data/brazil-states.geojson"
        brazil_states = json.loads(requests.get(url).text)

        cases_pd["SG_UF_NOT"] = cases_pd["SG_UF_NOT"].str.upper()

        # Create choropleth map
        fig = px.choropleth(
            cases_pd,
            geojson=brazil_states,
            locations="SG_UF_NOT",
            featureidkey="properties.sigla",
            color="count",
            # title="Número de Casos por UF",
        )

        fig.update_geos(fitbounds="locations", visible=False)
        fig.update_layout(template="plotly_white")

        return (fig.to_html(include_plotlyjs='cdn', full_html=False), cases_pd)

    def _plot_vaccination_rate(self, vaccinated_df: Optional[pd.DataFrame] = None, sort_by="Taxa de vacinados COVID (%)"):
        """
        Plot vaccination rates (COVID and Flu) by state using Plotly with vertical subplots.
        
        Args:
            vaccinated_df (pd.DataFrame): DataFrame containing state and vaccination rates. Expected columns: "UF", "Taxa de vacinados COVID (%)", "Taxa de vacinados Gripe (%)"
            sort_by (str): Column to sort states by (default "Taxa de vacinados COVID (%)").
        """
        if vaccinated_df is None:
            vaccinated_df = self.metric_calculator.calculate_vaccination_rate(group_by_state=True)
        # Sort by chosen column
        df = vaccinated_df.sort_values(sort_by, ascending=False)
        
        # Create vertical subplots (stacked)
        fig = make_subplots(rows=2, cols=1, shared_xaxes=True,
                            subplot_titles=("Vacina COVID", "Vacina Influenza"))
        
        # COVID barplot
        fig.add_trace(
            go.Bar(x=df["UF"], y=df["Taxa de vacinados COVID (%)"], text=df["Taxa de vacinados COVID (%)"], textposition="outside",
                marker_color="skyblue", name="COVID"),
            row=1, col=1
        )
        
        # Flu barplot
        fig.add_trace(
            go.Bar(x=df["UF"], y=df["Taxa de vacinados Gripe (%)"], text=df["Taxa de vacinados Gripe (%)"], textposition="outside",
                marker_color="lightgreen", name="Flu"),
            row=2, col=1 #, textfont_size=12
        )
        
        # Update layout
        fig.update_layout(
            # title_text="Taxa Pacientes com SRAG Vacinados por Estado",
            # title_x=0.5,
            height=800,
            width=700,
            showlegend=False
        )
        
        return (fig.to_html(include_plotlyjs='cdn', full_html=False), vaccinated_df)
    
    def generate_plot_data(self, query: str = None) -> dict:
        """
        This method computes key SRAG statistical dataframes used for visual analysis
        that will be available at SRAG report. It returns a dictionary
        containing the figure names and their dataframes sources.

        Generated Outputs Include:
            - plot_cases_per_month_last_year:
                Monthly SRAG case trends over the last 12 months.
            - plot_cases_per_day_last_month:
                Daily SRAG case progression during the most recent month.
            - plot_cases_by_uf:
                Choropleth map displaying SRAG case distribution of the last month by state (UF).
            - plot_vaccined_rate:
                Comparative chart of COVID-19 and Influenza vaccination rates by state
                during the last month.

        Returns:
            dict:A dictionary mapping plot identifiers to their respective computed SRAG statistics.
        """

        visual_data_results = {
            "plot_cases_per_month_last_year": self._plot_cases_per_month()[1].to_dict(orient="records"),
            "plot_cases_per_day_last_month": self._plot_cases_per_day()[1].to_dict(orient="records"),
            "plot_cases_by_uf": self._plot_cases_by_uf()[1].to_dict(orient="records"),
            "plot_vaccined_rate": self._plot_vaccination_rate()[1].to_dict(orient="records"),
        }
        return visual_data_results
    
    def generate_report_plots(self, query: str = None) -> dict:
        """This method generates plots for the SRAG report and returns a dictionary with the HTML strings.
        
        plot_cases_per_month_last_year: Plot of SRAG cases per month in the last year.
        plot_cases_per_day_last_month: Plot of SRAG cases per day in the last month.
        plot_cases_by_uf: Chloropletic plot of SRAG cases per UF.
        plot_vaccined_rate: Plot of COVID and Influenza vaccination rates of the last month by state.
        Returns:
            dict: HTML string containing all plots.
        """

        visual_results = {
            "plot_cases_per_month_last_year": self._plot_cases_per_month()[0],
            "plot_cases_per_day_last_month": self._plot_cases_per_day()[0],
            "plot_cases_by_uf": self._plot_cases_by_uf()[0],
            "plot_vaccined_rate": self._plot_vaccination_rate()[0],
        }
        return visual_results

# COMMAND ----------

class SRAGPlotsOutput(BaseModel):
    plot_cases_per_month_last_year: str = Field(..., description="Monthly SRAG case trends over the last 12 months")
    plot_cases_per_day_last_month: str = Field(..., description="Daily SRAG cases of last 30 days)")
    plot_cases_by_uf: str = Field(..., description="SRAG cases of the last month by UF")
    plot_vaccined_rate: str = Field(..., description="COVID/Flu vaccination rate per state")
