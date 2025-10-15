# Databricks notebook source
# MAGIC %md
# MAGIC ## **Dicionário dos dados**
# MAGIC
# MAGIC **NU_NOTIFIC**: Número da notificação sequencial gerado automaticamente pelo sistema. Primeiro dígito caracteriza o tipo da ficha (1=SG-Sindrome Gripal, 2=SRAG-UTI e 3-SRAG Hospitalizado).   
# MAGIC **DT_NOTIFIC**: Data da Notificação.   
# MAGIC **DT_SIN_PRI**: Data do primeiro sintoma.   
# MAGIC **SG_UF_NOT**: Unidade Federativa da Notificação.   
# MAGIC **ID_MUNICIP**: Município da Notificação.   
# MAGIC **EVOLUCAO**: Evolução do Caso(1-Cura, 2-Óbito, 3- Óbito por outras causas, 9-Ignorado).   
# MAGIC **DT_EVOLUCA**: Data da alta ou óbito.   
# MAGIC **CLASSI_FIN**: Classificação final do caso (1-SRAG por influenza, 2-SRAG por outro vírus respiratório, 4-SRAG não especificado, 5-SRAG por covid-19).   
# MAGIC **NU_IDADE_N**: Idade informada pelo paciente.   
# MAGIC **CS_SEXO**: Sexo.   
# MAGIC **FATOR_RISC**: Fatores de risco (1-Sim, 2-Não, 9-Ignorado).   
# MAGIC **CARDIOPATI**: Fatores de risco/ Doença Cardiovascular Crônica (1-Sim, 2-Não, 9-Ignorado).   
# MAGIC **DIABETES**: Fatores de risco/ Diabetes mellitus (1-Sim, 2-Não, 9-Ignorado).   
# MAGIC **IMUNODEPRE**: Fatores de risco/ Imunodeficiência ou Imunodepressão (1-Sim, 2-Não, 9-Ignorado).   
# MAGIC **OBESIDADE**: Fatores de risco/ Obesidade (1-Sim, 2-Não, 9-Ignorado).   
# MAGIC **HOSPITAL**: Houve internação? (1-Sim, 2-Não, 9-Ignorado).   
# MAGIC **DT_INTERNA**: Data da internação por SRAG.   
# MAGIC **UTI**: Internado em UTI? (1-Sim, 2-Não, 9-Ignorado).  
# MAGIC **DT_ENTUTI**: Data da entrada na UTI.   
# MAGIC **DT_SAIDUTI**: Data da saída da UTI.   
# MAGIC **SUPORT_VEN**: Uso de suporte ventilatório? (1-Sim invasivo, 2-Sim não invasivo, 3-Não, 9-Ignorado).   
# MAGIC **VACINA_COV**: Recebeu vacina COVID-19? (1-Sim, 2-Não, 9-Ignorado).   
# MAGIC **DOSE_1_COV**: Data 1ª dose da vacina COVID-19.   
# MAGIC **DOSE_2_COV**: Data 2ª dose da vacina COVID-19.   
# MAGIC **DOSE_REF**: Data da dose reforço da vacina COVID-19.    
# MAGIC **DOSE_2REF**: Data da 2ª dose reforço da vacina COVID-19.   
# MAGIC **FAB_COV_1**: Fabricante 1ª dose da vacina COVID-19.   
# MAGIC **FAB_COV_2**: Fabricante 2ª dose da vacina COVID-19.   
# MAGIC **FAB_RE_BI**:
# MAGIC **VACINA**: Recebeu vacina contra Gripe na última campanha? (1-Sim, 2-Não, 9-Ignorado).   
# MAGIC **DT_UT_DOSE**: Data da vacinação gripe.   
# MAGIC **MAE_VAC**: Se < 6 meses: a mãe recebeu a vacina? (1-Sim, 2-Nã, 9-Ignorado).    
# MAGIC **DT_VAC_MAE**: Se a mãe recebeu vacina, qual a data?   

# COMMAND ----------

!pip install --upgrade --quiet uv
!uv sync --active
dbutils.library.restartPython()

# COMMAND ----------

import json
import os
import toml
import pandas as pd
import plotly.express as px
import pyspark.sql.functions as F
import plotly.express as px
import requests
import numpy as np
import matplotlib.pyplot as plt
from datetime import timedelta
import pyspark.sql.types as T

# from utils.general_helpers import profile_dataframe
# os.getcwd()

# COMMAND ----------

# Load environment variables.
env_vars = toml.load("../../conf/env_vars.toml")

# Set as environment variables.
for key, value in env_vars.items():
    os.environ[key] = str(value)

# COMMAND ----------

srag_df = spark.read.table(F'{env_vars["CATALOG"]}.{env_vars["FS_SCHEMA"]}.srag_features')
hospital_df = spark.read.table(F'{env_vars["CATALOG"]}.{env_vars["FS_SCHEMA"]}.hospital_features')

# COMMAND ----------

hospital_df.limit(2).toPandas()

# COMMAND ----------

selected_columns = [
    "NU_NOTIFIC",
    "DT_NOTIFIC",
    "DT_SIN_PRI",
    "SG_UF_NOT",
    "ID_MUNICIP",
    # "CO_MUN_NOT",
    "EVOLUCAO",
    "DT_EVOLUCA",
    "CLASSI_FIN",
    "NU_IDADE_N",
    'TP_IDADE',
    "CS_SEXO",
    "FATOR_RISC",
    "CARDIOPATI",
    "DIABETES",
    "IMUNODEPRE",
    "OBESIDADE",
    "HOSPITAL",
    "DT_INTERNA",
    "UTI",
    "DT_ENTUTI",
    "DT_SAIDUTI",
    # "ID_UNIDADE",
    # "CO_UN_INTE",
    "SUPORT_VEN",
    "VACINA_COV",
    "DOSE_1_COV",
    "DOSE_2_COV",
    "DOSE_REF",
    "DOSE_2REF",
    # "FAB_COV_1",
    # "FAB_COV_2",
    # "FAB_COVRF",
    # "FAB_COVRF2",
    # "FAB_RE_BI",
    "VACINA",
    "DT_UT_DOSE",
    "MAE_VAC",
    "DT_VAC_MAE"
]

# COMMAND ----------

# srag_df_profile = profile_dataframe(srag_df)
# srag_df_profile

# COMMAND ----------

display(
  srag_df
  .limit(5)
  .toPandas()
)

# COMMAND ----------

from typing import Dict, Any
from langchain.tools import BaseTool
from pydantic import BaseModel, Field
from pyspark.sql.connect.dataframe import DataFrame

# COMMAND ----------

# MAGIC %md
# MAGIC ## Metric Class

# COMMAND ----------

import pandas as pd
import json
from typing import Optional
from pyspark.sql.connect.dataframe import DataFrame

class SRAGMetrics:
    def __init__(
        self, 
        df_srag: Optional[DataFrame] = None, 
        df_hospital: Optional[DataFrame] = None):
        """
        df_srag: optional main dataset (e.g., monthly cases)
        df_hospital: optional secondary dataset (e.g., hospitalizations)
        """
        catalog = os.environ["CATALOG"]
        schema = os.environ["FS_SCHEMA"]
        self.df_srag = df_srag if df_srag is not None else spark.read.table(f'{catalog}.{schema}.srag_features')

        self.df_hospital = df_hospital if df_hospital is not None else spark.read.table(f'{catalog}.{schema}.hospital_features')

    # Metric functions
    def calculate_cases_per_month(
        self, 
        start_date: Optional[str] = None, 
        end_date: Optional[str] = None
        ) -> pd.DataFrame:
        """
        Calculates number of cases per month. If start and end_dates are not provided, use last 12 months.
        
        Args:
            df (DataFrame): Spark DataFrame with column DT_NOTIFIC (date).
            start_date (str): Start date in 'yyyy-MM-dd'. If None, defaults to 12 months ago.
            end_date (str): End date in 'yyyy-MM-dd'. If None, defaults to today.
            
        Returns:
            Pandas DataFrame with ['year_month', 'count'].
        """
        # Default period = last 12 months.
        if end_date is None:
            end_date = pd.to_datetime("today").strftime("%Y-%m-%d")
        if start_date is None:
            start_date = (pd.to_datetime(end_date) - pd.DateOffset(months=12)).strftime("%Y-%m-%d")
        # Filter Spark DataFrame.
        df_filtered = self.df_srag.filter((F.col("DT_NOTIFIC") >= F.lit(start_date)) & 
                            (F.col("DT_NOTIFIC") <= F.lit(end_date)))      
        # Aggregate cases per month.
        cases_per_month = (
            df_filtered
            .withColumn("year_month", F.date_format("DT_NOTIFIC", "yyyy-MM"))
            .groupBy("year_month")
            .count()
            .orderBy("year_month")
        )
        # Convert to Pandas
        cases_pd = cases_per_month.toPandas()
        cases_pd["year_month"] = pd.to_datetime(cases_pd["year_month"])
        return cases_pd
    
    def calculate_cases_per_month_variation_rate(
        self,
        cases_current_count: Optional[pd.DataFrame] = None, 
        cases_comparison_count: Optional[pd.DataFrame] = None
        ) -> float:
        """
        Calculates the increase rate of cases per month. If cases_current_count and cases_comparison_count are not provided, use last month and 12 months ago.
        
        Args:
            cases_current_count: number of srag cases in the current month. If None, defaults number of the last month.
            cases_comparison_count: number of srag cases in the previous period to compare with. If None, defaults to 12 months ago.
        Returns:
            increase_rate (float): increase rate of cases compared to the previos period.
        """
        last_month = pd.to_datetime("today") - pd.DateOffset(months=1)
        one_year_before = pd.to_datetime("today") - pd.DateOffset(months=13)

        if cases_current_count is None:
            cases_current_count = self.calculate_cases_per_month(
                start_date=pd.offsets.MonthBegin().rollback(last_month), 
                end_date=pd.offsets.MonthEnd().rollforward(last_month))["count"][0]
        if cases_comparison_count is None:
            cases_comparison_count = self.calculate_cases_per_month(
                start_date=pd.offsets.MonthBegin().rollback(one_year_before), 
                end_date=pd.offsets.MonthEnd().rollforward(one_year_before))["count"][0]

        variation_rate = ((cases_current_count - cases_comparison_count) / cases_comparison_count).round(2)*100
        return variation_rate
    
    def calculate_cases_per_day(
        self, 
        start_date: Optional[str] = None, 
        end_date: Optional[str] = None,
        ) -> pd. DataFrame :
        """
        Calculates number of cases per day.
        
        Args:
            df (DataFrame): Spark DataFrame with column DT_NOTIFIC (date) of the period of time to calculate the daily number of SRAG cases.
            start_date (str): Start date in 'yyyy-MM-dd'. If None, defaults to 30 days interval.
            end_date (str): End date in 'yyyy-MM-dd'. If None, defaults to today.
            
        Returns:
            Pandas DataFrame with ['DT_NOTIFIC', 'count'].
        """
        
        # Default period = last 30 days
        if end_date is None:
            end_date = pd.to_datetime("today").strftime("%Y-%m-%d")
        if start_date is None:
            start_date = (pd.to_datetime(end_date) - pd.DateOffset(days=30)).strftime("%Y-%m-%d")

        # Filter Spark DataFrame to the period of time desired.
        filtered = self.df_srag.filter((F.col("DT_NOTIFIC") >= F.lit(start_date)) & 
                            (F.col("DT_NOTIFIC") <= F.lit(end_date)))
        
        # Aggregate cases per day
        cases_per_day = (
            filtered
            .groupBy("DT_NOTIFIC")
            .count()
            .orderBy("DT_NOTIFIC")
        )
        
        # Convert to Pandas
        cases_per_day_pd = cases_per_day.toPandas()
        cases_per_day_pd["DT_NOTIFIC"] = pd.to_datetime(cases_per_day_pd["DT_NOTIFIC"])

        return cases_per_day_pd

    # Agent-facing run method
    def run(self, query: str = None) -> str:
        results = {
            # "cases_per_month": self.calculate_cases_per_month().to_dict(orient="records"),
            "calculate_cases_per_month_variation_rate": self.calculate_cases_per_month_variation_rate(),
            # "hospitalizations": self.get_hospitalizations().to_dict(orient="records"),
            # "summary": self.get_summary().to_dict(orient="records")
        }
        return json.dumps(results, indent=2)

# COMMAND ----------

SRAGMetrics().run()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Plot Class

# COMMAND ----------

class SRAGMVisualization:
    def __init__(
        self, 
        df_srag: Optional[DataFrame] = None, 
        df_hospital: Optional[DataFrame] = None,
        metric_calculator: Optional["SRAGMetrics"] = None) -> None:
        """
        df_srag: optional main dataset (e.g., monthly cases)
        df_hospital: optional secondary dataset (e.g., hospitalizations)
        """
        catalog = os.environ["CATALOG"]
        schema = os.environ["FS_SCHEMA"]
        self.df_srag = df_srag if df_srag is not None else spark.read.table(f'{catalog}.{schema}.srag_features')

        self.df_hospital = df_hospital if df_hospital is not None else spark.read.table(f'{catalog}.{schema}.hospital_features')

        self.metric_calculator = metric_calculator if metric_calculator is not None else SRAGMetrics()

    # Visualization functions
    def plot_cases_per_month(
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
            title=title,
            markers=True,
            labels={"year_month": "Mês", "count": "Número de casos"}
        )
        
        fig.update_layout(
            xaxis_title="Mês",
            yaxis_title="Número de casos",
            xaxis=dict(dtick="M1", tickformat="%b\n%Y"),
            template="plotly_white"
        )
        fig.write_html("../../data/visualizations/cases_per_month.html")
        fig.show()

    
    def plot_cases_per_day(
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
            cases_per_day_pd = self.metric_calculator.calculate_cases_per_day()
        
        # Ensure correct datetime type.
        cases_per_day_pd["DT_NOTIFIC"] = pd.to_datetime(cases_per_day_pd["DT_NOTIFIC"])
        print("a")
        last_date = cases_per_day_pd["DT_NOTIFIC"].max()
        first_date = cases_per_day_pd["DT_NOTIFIC"].min()
        end_30_days_interval = first_date + timedelta(days=30)
        print("b")
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
            title=title,
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
        fig.write_html("../../data/visualizations/cases_per_day.html")
        fig.show()
    
    def run(self, query: str = None) -> str:
        results = {
            "plot_cases_per_month_last_year": self.plot_cases_per_month(),
            "plot_cases_per_day_last_month": self.plot_cases_per_day(),
        }
        return json.dumps(results, indent=2)

# COMMAND ----------

SRAGMVisualization(SRAGMetrics()).run()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Número de Casos por mês

# COMMAND ----------

def calculate_cases_per_month(df, start_date=None, end_date=None):
    """
    Calculates number of cases per month.
    
    Args:
        df (DataFrame): Spark DataFrame with column DT_NOTIFIC (date).
        start_date (str): Start date in 'yyyy-MM-dd'. If None, defaults to 12 months ago.
        end_date (str): End date in 'yyyy-MM-dd'. If None, defaults to today.
        
    Returns:
        Pandas DataFrame with ['year_month', 'count'].
    """
    
    # Default period = last 12 months
    if end_date is None:
        end_date = pd.to_datetime("today").strftime("%Y-%m-%d")
    if start_date is None:
        start_date = (pd.to_datetime(end_date) - pd.DateOffset(months=12)).strftime("%Y-%m-%d")

    # Filter Spark DataFrame
    filtered = df.filter((F.col("DT_NOTIFIC") >= F.lit(start_date)) & 
                         (F.col("DT_NOTIFIC") <= F.lit(end_date)))
    
    # Aggregate cases per month
    cases_per_month = (
        filtered
        .withColumn("year_month", F.date_format("DT_NOTIFIC", "yyyy-MM"))
        .groupBy("year_month")
        .count()
        .orderBy("year_month")
    )
    
    # Convert to Pandas
    cases_pd = cases_per_month.toPandas()
    cases_pd["year_month"] = pd.to_datetime(cases_pd["year_month"])
    
    return cases_pd

# COMMAND ----------

def plot_cases_per_month(cases_pd, title="Número de casos por mês"):
    """
    Plots a single time series of cases per month.
    
    Args:
        cases_pd (DataFrame): Pandas DataFrame with ['year_month', 'count'].
        title (str): Plot title.
    """
    
    # Ensure correct datetime type
    cases_pd["year_month"] = pd.to_datetime(cases_pd["year_month"])
    
    # Plot with Plotly
    fig = px.line(
        cases_pd,
        x="year_month",
        y="count",
        title=title,
        markers=True,
        labels={"year_month": "Mês", "count": "Número de casos"}
    )
    
    fig.update_layout(
        xaxis_title="Mês",
        yaxis_title="Número de casos",
        xaxis=dict(dtick="M1", tickformat="%b\n%Y"),
        template="plotly_white"
    )
    
    fig.show()


# COMMAND ----------

cases_current = calculate_cases_per_month(srag_df)  # from the earlier function
plot_cases_per_month(cases_current)


# COMMAND ----------

agosto_2025 = calculate_cases_per_month(srag_df, start_date="2025-08-01", end_date="2025-08-31")
julho_2025 =  calculate_cases_per_month(srag_df, start_date="2025-07-01", end_date="2025-07-31")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Taxa de variação de casos por mês

# COMMAND ----------

agosto_2025

# COMMAND ----------

julho_2025

# COMMAND ----------

def calculate_cases_per_month_variation_rate(cases_current_count, cases_comparison_count):
    """
    Calculates the increase rate of cases per month.
    
    Args:
        cases_current_count: number of srag cases in the current month.
        cases_comparison_count: number of srag cases in the previous period to compare with.
        count'].

    Returns:
        increase_rate (float): increase rate of cases compared to the previos period.
    """
    variation_rate = ((cases_current_count - cases_comparison_count) / cases_comparison_count).round(2)*100
    return variation_rate   
    

# COMMAND ----------

calculate_cases_per_month_variation_rate(agosto_2025["count"][0], julho_2025["count"][0])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Número de casos por estado

# COMMAND ----------


# Aggregate cases by UF
cases_per_uf = (
    srag_df
    .dropna(subset=["SG_UF_NOT"])
    .groupBy("SG_UF_NOT")
    .count()
    .orderBy(F.desc("count"))
)
cases_pd = cases_per_uf.toPandas()

# Load Brazil states GeoJSON
url = "https://raw.githubusercontent.com/codeforamerica/click_that_hood/master/public/data/brazil-states.geojson"
brazil_states = json.loads(requests.get(url).text)

# Ensure UF column is uppercase and matches keys in GeoJSON
cases_pd["SG_UF_NOT"] = cases_pd["SG_UF_NOT"].str.upper()

# Plot
fig = px.choropleth(
    cases_pd,
    geojson=brazil_states,
    locations="SG_UF_NOT",
    featureidkey="properties.sigla",  # key inside the GeoJSON (uses 'sigla' for UF code)
    color="count",
    color_continuous_scale="Hot",
    title="Número de casos por UF",
)

fig.update_geos(fitbounds="locations", visible=False)
fig.update_layout(template="plotly_white")
fig.show()


# COMMAND ----------

def plot_cases_by_uf(srag_df):
    import json
    import requests
    import plotly.express as px
    from pyspark.sql import functions as F

    # 1️⃣ Agregar casos por UF
    cases_per_uf = (
        srag_df
        .dropna(subset=["SG_UF_NOT"])
        .groupBy("SG_UF_NOT")
        .count()
        .orderBy(F.desc("count"))
    )
    cases_pd = cases_per_uf.toPandas()

    # 2️⃣ Carregar GeoJSON dos estados do Brasil
    url = "https://raw.githubusercontent.com/codeforamerica/click_that_hood/master/public/data/brazil-states.geojson"
    brazil_states = json.loads(requests.get(url).text)

    # 3️⃣ Garantir UF em maiúsculas para combinar com o GeoJSON
    cases_pd["SG_UF_NOT"] = cases_pd["SG_UF_NOT"].str.upper()

    # 4️⃣ Criar mapa coroplético
    fig = px.choropleth(
        cases_pd,
        geojson=brazil_states,
        locations="SG_UF_NOT",
        featureidkey="properties.sigla",  # chave do GeoJSON
        color="count",
        title="Número de Casos por UF",
    )

    # 5️⃣ Ajustes visuais
    fig.update_geos(fitbounds="locations", visible=False)
    fig.update_layout(template="plotly_white")

    return fig  # retornar figura para possível exibição externa (fig.show() opcional)


# COMMAND ----------

plot_cases_by_uf(srag_df)

# COMMAND ----------



# COMMAND ----------

plot_srag_rate_by_uf(srag_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Número de casos por dia

# COMMAND ----------

def calculate_cases_per_day(df, start_date=None, end_date=None):
    """
    Calculates number of cases per day.
    
    Args:
        df (DataFrame): Spark DataFrame with column DT_NOTIFIC (date).
        start_date (str): Start date in 'yyyy-MM-dd'. If None, defaults to 30 days interval.
        end_date (str): End date in 'yyyy-MM-dd'. If None, defaults to today.
        
    Returns:
        Pandas DataFrame with ['DT_NOTIFIC', 'count'].
    """
    
    # Default period = last 30 days
    if end_date is None:
        end_date = pd.to_datetime("today").strftime("%Y-%m-%d")
    if start_date is None:
        start_date = (pd.to_datetime(end_date) - pd.DateOffset(days=30)).strftime("%Y-%m-%d")

    # Filter Spark DataFrame
    filtered = df.filter((F.col("DT_NOTIFIC") >= F.lit(start_date)) & 
                         (F.col("DT_NOTIFIC") <= F.lit(end_date)))
    
    # Aggregate cases per day
    cases_per_day = (
        filtered
        .groupBy("DT_NOTIFIC")
        .count()
        .orderBy("DT_NOTIFIC")
    )
    
    # Convert to Pandas
    cases_pd = cases_per_day.toPandas()
    cases_pd["DT_NOTIFIC"] = pd.to_datetime(cases_pd["DT_NOTIFIC"])
    
    return cases_pd

# COMMAND ----------

def plot_cases_per_day(cases_pd, title="Número de casos por dia dos últimos 30 dias"):
    """
    Plots a single time series of cases per day.
    
    Args:
        cases_pd (DataFrame): Pandas DataFrame with ['DT_NOTIFIC', 'count'].
        title (str): Plot title.
    """
    
    # Ensure correct datetime type
    cases_pd["DT_NOTIFIC"] = pd.to_datetime(cases_pd["DT_NOTIFIC"])
    last_date = cases_pd["DT_NOTIFIC"].max()
    first_date = cases_pd["DT_NOTIFIC"].min()
    end_30_days_interval = first_date + timedelta(days=30)

    # Create full date range
    full_range = pd.date_range(start=first_date, end=end_30_days_interval, freq="D")
    
    # Reindex to include missing days with 0
    cases_pd = (
        cases_pd.set_index("DT_NOTIFIC")
        .reindex(full_range, fill_value=0)
        .rename_axis("DT_NOTIFIC")
        .reset_index()
    )

    
    # Plot with Plotly
    fig = px.line(
        cases_pd,
        x="DT_NOTIFIC",
        y="count",
        title=title,
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
            y=cases_pd["count"].max(),
            text=f"Último dado disponível: {last_date.strftime('%d/%m/%Y')}",
            showarrow=False,
            bgcolor="white"
        )
    
    fig.show()

# COMMAND ----------

cases_30_days = calculate_cases_per_day(srag_df)
plot_cases_per_day(cases_30_days)

# COMMAND ----------

# MAGIC %md
# MAGIC #Feature Engineering

# COMMAND ----------

srag_df = srag_df.withColumns({
    "obito_srag": F.when(F.col("EVOLUCAO") == 2, 1).otherwise(0),
    "alta": F.when(F.col("EVOLUCAO") == 1, 1).otherwise(0),
    "dias_internacao_uti": F.when(F.col("DT_SAIDUTI").isNotNull(), F.datediff(F.col("DT_SAIDUTI"), F.col("DT_ENTUTI"))).otherwise(F.datediff(F.col("DT_EVOLUCA"), F.col("DT_ENTUTI"))),
    "idade_anos": F.when(F.col("TP_IDADE") == 1, F.round(F.col("NU_IDADE_N")/365, 2)).when(F.col("TP_IDADE") == 2, F.round(F.col("NU_IDADE_N")/12, 2)).otherwise(F.col("NU_IDADE_N")),
    }).withColumns({
        "classificacao_etaria_leito": F.when(F.col("idade_anos") <= 0.0768, F.lit("neonatal")).when(F.col("idade_anos") >= 12, F.lit("adulto")).otherwise(F.lit("pediatrica")),
    })

# COMMAND ----------

srag_df.filter(F.col("classificacao_etaria_leito").isNull()).count()


# COMMAND ----------

srag_df.limit(10).toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC # Metrics

# COMMAND ----------

# MAGIC %md
# MAGIC Principais:
# MAGIC - Taxa de Aumento de Casos: 
# MAGIC
# MAGIC - Taxa de mortalidade
# MAGIC
# MAGIC - Taxa de Ocupação de UTI
# MAGIC
# MAGIC - Taxa de Vacinação da População
# MAGIC
# MAGIC Primárias:
# MAGIC - Número de Casos por Dia: Contagem 
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Taxa internação UTI

# COMMAND ----------

def taxa_internacao_uti(df, start_date, end_date):
    """
    Calcula taxa de internação em UTI com relação ao número de casos de SRAG por mês, ao longo do período selecionado.
    
    Args:
        df (DataFrame): Spark DataFrame com colunas DT_NOTIFIC, DT_ENTUTI, DT_SAIDUTI.
        start_date (str): Data inicial (yyyy-MM-dd).
        end_date (str): Data final (yyyy-MM-dd).
        
    Returns:
        Pandas DataFrame com colunas [year_month, casos, internados, taxa_internacao].
    """
    
    # Filtro do período
    df_filtered = df.filter(
        (F.col("DT_NOTIFIC") >= F.lit(start_date)) & 
        (F.col("DT_NOTIFIC") <= F.lit(end_date))
    )
    
    # ---------------------------
    # Casos por mês (base DT_NOTIFIC)
    # ---------------------------
    casos = (
        df_filtered
        .withColumn("year_month", F.date_format("DT_NOTIFIC", "yyyy-MM"))
        .groupBy("year_month")
        .agg(F.count("*").alias("casos"))
    )
    
    # ---------------------------
    # Internados UTI por mês
    # ---------------------------
    # Criar coluna de intervalo por mês
    meses = pd.date_range(start=start_date, end=end_date, freq="MS")  # MS = month start
    
    internados_list = []
    for m in meses:
        m_start = pd.to_datetime(m).strftime("%Y-%m-%d")
        m_end = (pd.to_datetime(m) + pd.offsets.MonthEnd(0)).strftime("%Y-%m-%d")
        
        # Condição: internado no mês (Data de entrada ou saída de internação na UTI dentro do período selecionado, ou data de entrada na UTI anterior ao período selecionado e data saída da UTI superior ao período selecionado)
        cond = (
            ((F.col("DT_ENTUTI") >= F.lit(m_start)) & (F.col("DT_ENTUTI") <= F.lit(m_end))) |
            ((F.col("DT_SAIDUTI") >= F.lit(m_start)) & (F.col("DT_SAIDUTI") <= F.lit(m_end))) |
            ((F.col("DT_ENTUTI") <= F.lit(m_start)) & (F.col("DT_SAIDUTI") >= F.lit(m_end)))
        )
        
        count_internados = df.filter(cond).count()
        internados_list.append({"year_month": m.strftime("%Y-%m"), "internados": count_internados})
    
    internados_pd = pd.DataFrame(internados_list)
    
    # ---------------------------
    # Unir casos + internados
    # ---------------------------
    result = casos.toPandas().merge(internados_pd, on="year_month", how="outer").fillna(0)
    
    # Calcular taxa de ocupação
    result["taxa_internacao"] = round((result["internados"] / result["casos"].replace(0, pd.NA))*100, 2)
    
    return result.sort_values("year_month")


# COMMAND ----------

resultado = taxa_internacao_uti(srag_df, "2025-03-01", "2025-03-31")
print(resultado)

# COMMAND ----------

# MAGIC %md
# MAGIC # Comparação número de casos mensais de SRAG

# COMMAND ----------

resultado_3m = calculate_cases_per_month(srag_df, "2025-06-01", "2025-09-10")
resultado_3m["variation_rate_month"] = (calculate_cases_per_month_variation_rate(resultado_3m["count"], resultado_3m["count"].shift(1)))

resultado_3m_2024 = calculate_cases_per_month(srag_df, "2024-06-01", "2024-09-10")

merge_results = resultado_3m.merge(resultado_3m_2024, right_index=True, left_index=True)
merge_results["variation_rate_year"] = calculate_cases_per_month_variation_rate(merge_results["count_x"], merge_results["count_y"])
merge_results

# COMMAND ----------


months = merge_results["year_month_x"].dt.month
this_year = merge_results["count_x"].to_numpy()
last_year = merge_results["count_y"].to_numpy()
this_year_label = f'{merge_results["year_month_x"].dt.year.unique()}'
last_year_label = f'{merge_results["year_month_y"].dt.year.unique()}'
month_rate_variation = merge_results["variation_rate_month"].to_numpy()

x = np.arange(len(months))
width = 0.35

fig, ax = plt.subplots(figsize=(8, 5))

# Bars
rects1 = ax.bar(x - width/2, last_year, width, label=last_year_label, color="skyblue")
rects2 = ax.bar(x + width/2, this_year, width, label=this_year_label, color="dodgerblue")

ax.set_ylabel("Número de casos notificados")
ax.set_xlabel("Mês")
ax.set_title("Comparação Números de Caso Mensais")
ax.set_xticks(x)
ax.set_xticklabels(months)
ax.legend()

# Add MoM % with elbow arrow AND text above the current bar
for i in range(1, len(month_rate_variation)):
    # Draw elbow arrow (up → right)
    ax.annotate("",
                xy=(x[i]+ width/6, this_year[i]*1.08), 
                xytext=(x[i-1] + width/2, this_year[i-1]),
                textcoords="data",
                arrowprops=dict(
                    arrowstyle="-|>", 
                    color="black",
                    connectionstyle="angle,angleA=90,angleB=0,rad=0"
                ))

    # Place variation % above the current bar
    ax.text(
        x[i] + width/2, this_year[i]*1.05 + 30,
        f"{month_rate_variation[i]:.0f}%",
        ha="center", va="bottom", fontsize=11, color="black", fontweight="bold"
    )

plt.tight_layout()
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC # Taxa Ocupação UTI por estado 

# COMMAND ----------

# def calculate_uci_occupancy_count_per_state(month_year, srag_df):

schema = T.StructType(
    [
        T.StructField("adulto", T.IntegerType(), True),
        T.StructField("pediatrica", T.IntegerType(), True),
        T.StructField("neonatal", T.IntegerType(), True),     
    ]
)
last_date = srag_df.select(F.max("DT_NOTIFIC")).collect()[0][0]
start_date = last_date.replace(day=1)

srag_filtered_1 = srag_df.select(["DT_ENTUTI", "DT_SAIDUTI", "DT_EVOLUCA", "EVOLUCAO", "SG_UF_NOT", "classificacao_etaria_leito"]).filter((F.year("DT_ENTUTI") == 2025) | (F.year("DT_SAIDUTI") == 2025))

states = [row["SG_UF_NOT"] for row in srag_filtered_1.select("SG_UF_NOT").distinct().collect()]

# Empty DataFrame with full schema
patients_df = spark.createDataFrame([], schema)

cond = (
((F.col("DT_ENTUTI") >= F.lit(start_date)) & (F.col("DT_ENTUTI") <= F.lit(last_date))) |
((F.col("DT_SAIDUTI") >= F.lit(start_date)) & (F.col("DT_SAIDUTI") <= F.lit(last_date))) |
((F.col("DT_ENTUTI") <= F.lit(start_date)) & (F.col("DT_SAIDUTI") >= F.lit(last_date)))
)
state_uci_bed_sum = (
    srag_filtered_1.filter(cond).groupBy("SG_UF_NOT")
    .pivot("classificacao_etaria_leito")
    .count()
    .withColumn("uf_month_year", F.concat(F.lit(F.col("SG_UF_NOT")), F.lit(F"{last_date.year}{last_date.month:02d}"))))    
patients_df = patients_df.unionByName(state_uci_bed_sum, allowMissingColumns=True)
patients_df = patients_df.toPandas().fillna(0)

# COMMAND ----------

patients_df

# COMMAND ----------


srag_filtered_1 = srag_df.select(["DT_ENTUTI", "DT_SAIDUTI", "DT_EVOLUCA", "EVOLUCAO", "SG_UF_NOT", "classificacao_etaria_leito"]).filter((F.year("DT_ENTUTI") == 2025) | (F.year("DT_SAIDUTI") == 2025))
month_year_list = pd.date_range(start=month_year_start, end=month_year_end, freq="MS")
states = [row["SG_UF_NOT"] for row in srag_filtered_1.select("SG_UF_NOT").distinct().collect()]

schema = T.StructType(
    [
        T.StructField("adulto", T.IntegerType(), True),
        T.StructField("pediatrica", T.IntegerType(), True),
        T.StructField("neonatal", T.IntegerType(), True),     
        T.StructField("month_year", T.StringType(), True),
    ]
)

# Empty DataFrame with full schema
all_results = spark.createDataFrame([], schema)

# for state in states:
#     state_srag_uci_beds = srag_filtered_1.filter(F.col("SG_UF_NOT") == state)
for month_year in month_year_list:
    m_start = pd.to_datetime(month_year).strftime("%Y-%m-%d")
    m_end = (pd.to_datetime(month_year) + pd.offsets.MonthEnd(0)).strftime("%Y-%m-%d")
    cond = (
    ((F.col("DT_ENTUTI") >= F.lit(m_start)) & (F.col("DT_ENTUTI") <= F.lit(m_end))) |
    ((F.col("DT_SAIDUTI") >= F.lit(m_start)) & (F.col("DT_SAIDUTI") <= F.lit(m_end))) |
    ((F.col("DT_ENTUTI") <= F.lit(m_start)) & (F.col("DT_SAIDUTI") >= F.lit(m_end)))
    )
    state_uci_bed_sum = (
    srag_filtered_1.filter(cond).groupBy("SG_UF_NOT")
    .pivot("classificacao_etaria_leito")
    .count()
    .withColumns({
        "month_year": F.lit(month_year),
        "uf_month_year": F.concat(F.lit(F.col("SG_UF_NOT")), F.lit(F"{month_year.year}{month_year.month:02d}"))
    }))    
    all_results = all_results.unionByName(state_uci_bed_sum, allowMissingColumns=True)
all_results = all_results.toPandas().fillna(0)

# COMMAND ----------

hospital_df.limit(2).toPandas()

# COMMAND ----------

def calculate_uci_occupancy_per_state(srag_df, hospital_df):
    """
    Calculates UCI bed occupancy rate of the current month (or month of last data available)).
    
    Args:
        srag_df (pd.DataFrame): DataFrame with srag df notification data.
        hospital_df (pd.DataFrame): DataFrame with bed counts per category (adulto, pediatrica, neonatal)
                                + keys ['month_year', 'SG_UF_NOT'].
                                
    Returns:
        pd.DataFrame with occupancy rates per category and overall.
    """

    # Calculate uci bead occupancy rate per state and category.
    schema = T.StructType(
    [
        T.StructField("adulto", T.IntegerType(), True),
        T.StructField("pediatrica", T.IntegerType(), True),
        T.StructField("neonatal", T.IntegerType(), True),     
        ]
    )
    last_date = srag_df.select(F.max("DT_NOTIFIC")).collect()[0][0]
    start_date = last_date.replace(day=1)
    srag_filtered_1 = srag_df.select(["DT_ENTUTI", "DT_SAIDUTI", "DT_EVOLUCA", "EVOLUCAO", "SG_UF_NOT", "classificacao_etaria_leito"]).filter((F.year("DT_ENTUTI") == 2025) | (F.year("DT_SAIDUTI") == 2025))
    states = [row["SG_UF_NOT"] for row in srag_filtered_1.select("SG_UF_NOT").distinct().collect()]
    patients_df = spark.createDataFrame([], schema)
    # Calculate number of patients were admitted to UCI in the last month or admitted in this year but did not leave yeat.
    cond = (
    ((F.col("DT_ENTUTI") >= F.lit(start_date)) & (F.col("DT_ENTUTI") <= F.lit(last_date))) |
    ((F.col("DT_SAIDUTI") >= F.lit(start_date)) & (F.col("DT_SAIDUTI") <= F.lit(last_date))) |
    ((F.col("DT_ENTUTI") <= F.lit(start_date)) & (F.col("DT_SAIDUTI") >= F.lit(last_date)))
    )
    state_uci_bed_sum = (
        srag_filtered_1.filter(cond).groupBy("SG_UF_NOT")
        .pivot("classificacao_etaria_leito")
        .count())   
    patients_df = patients_df.unionByName(state_uci_bed_sum, allowMissingColumns=True)
    patients_df = patients_df.toPandas().fillna(0).rename(columns={"SG_UF_NOT": "uf"}) 

    # Filter UCI beds available in the last month.
    # Get max month_year
    max = hospital_df.select(F.max("month_year")).collect()[0][0]
    hospital_df_filtered  = hospital_df.filter(
        F.col("month_year") == max).withColumnRenamed("UF", "uf").toPandas()
    hospital_df_filtered = hospital_df_filtered[["uf", "adulto", "pediatrica", "neonatal"]]

    # Merge patient_df (uci ocupancy) with hospital_df_filtered (uci bed availables per state).
    merged = patients_df.merge(
        hospital_df_filtered,
        on="uf",
        suffixes=("_patients", "_beds")
    )

    # Calculate rates per category
    for uci_cat in ["adulto", "pediatrica", "neonatal"]:
        merged[f"{uci_cat}_rate"] = (
            merged[f"{uci_cat}_patients"] / merged[f"{uci_cat}_beds"] * 100
        ).round(2)

    # Calculate overall totals.
    merged["total_patients"] = (
        merged["adulto_patients"] + merged["pediatrica_patients"] + merged["neonatal_patients"]
    )
    merged["total_beds"] = (
        merged["adulto_beds"] + merged["pediatrica_beds"] + merged["neonatal_beds"]
    )
    merged["total_rate"] = (
        merged["total_patients"] / merged["total_beds"] * 100
    ).round(2)
    merged["month_year"] = start_date.strftime('%m/%Y')

    uci_ocuppancy_per_state_df = merged[
        ["month_year","uf", "adulto_rate", "pediatrica_rate", "neonatal_rate", "total_rate"]
        ].rename(columns={"month_year": "Mês/Ano", "uf": "UF", "adulto_rate": "Taxa de ocupação UTI adulto", "pediatrica_rate": "Taxa de ocupação UTI pediátrica", "neonatal_rate": "Taxa de ocupação UTI neonatal", "total_rate": "Taxa de ocupação total UTIs"})
    uci_ocuppancy_per_state_df[['Taxa de ocupação UTI adulto',
        'Taxa de ocupação UTI pediátrica', 'Taxa de ocupação UTI neonatal',
        'Taxa de ocupação total UTIs']] = uci_ocuppancy_per_state_df[['Taxa de ocupação UTI adulto',
        'Taxa de ocupação UTI pediátrica', 'Taxa de ocupação UTI neonatal',
        'Taxa de ocupação total UTIs']].clip(upper=100)

    uci_ocuppancy_per_state_df = uci_ocuppancy_per_state_df.sort_values(by="UF")
    return uci_ocuppancy_per_state_df



# COMMAND ----------

a = calculate_uci_occupancy_per_state(srag_df, hospital_df)
a

# COMMAND ----------

# MAGIC %md
# MAGIC # Taxa Vacinação COVID e Gripe

# COMMAND ----------

def calculate_vaccination_rate(srag_df, group_by_state=False):
    """
    Calculate the percentage of vaccinated patients over the number of SRAG notifications by month.
    
    Args:
        srag_df (DataFrame): Input Spark DataFrame with columns VACINA_COV, NU_NOTIFIC, DT_NOTIFIC, UF
        group_by_state (bool): If True, group results by state as well
    
    Returns:
        DataFrame: Spark DataFrame with month (and state if selected), total_notifications,
                   vaccinated, percentage_vaccinated
    """
    
    # Extract year-month from notification date
    df = srag_df.withColumn("year_month", F.date_format("DT_NOTIFIC", "yyyy-MM"))
    if group_by_state:
        group = ["year_month", "SG_UF_NOT"]
    else:
        group = ["year_month"]
    # Count total notifications
    total_df = df.groupBy(group) \
                 .agg({"NU_NOTIFIC" : "count",
                       "vacinacao_covid" : "sum",
                       "vacinacao_gripe": "sum"})
    total_df = (total_df.withColumnRenamed("count(NU_NOTIFIC)", "total_notifications")
                        .withColumnRenamed("sum(vacinacao_covid)", "vaccinated_covid")
                        .withColumnRenamed("sum(vacinacao_gripe)", "vaccinated_gripe"))
    total_df = total_df.withColumns({
        "rate_vaccinated_covid": F.round((F.col("vaccinated_covid") / F.col("total_notifications") * 100), 2),
        "rate_vaccinated_gripe": F.round((F.col("vaccinated_gripe") / F.col("total_notifications") * 100), 2)
        })
    
    return total_df


# COMMAND ----------

vaccination_rate_per_state = calculate_vaccination_rate(
    srag_df.filter((F.year("DT_NOTIFIC") == 2025)& (F.month("DT_NOTIFIC") == 8)), group_by_state=True).toPandas()

# COMMAND ----------

vaccination_rate_per_state

# COMMAND ----------

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

def plot_vaccination_rates_plotly(df, state_col="SG_UF_NOT", covid_col="rate_vaccinated_covid", flu_col="rate_vaccinated_gripe", sort_by="rate_vaccinated_covid"):
    """
    Plot vaccination rates (COVID and Flu) by state using Plotly with vertical subplots.
    
    Args:
        df (pd.DataFrame): DataFrame containing state and vaccination rates.
        state_col (str): Column name for states.
        covid_col (str): Column name for COVID vaccination rate.
        flu_col (str): Column name for Flu vaccination rate.
        sort_by (str): Column to sort states by (default 'covid_rate').
    """
    
    # Sort by chosen column
    df = df.sort_values(sort_by, ascending=False)
    
    # Create vertical subplots (stacked)
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True,
                        subplot_titles=("COVID Vaccination Rate by State", "Flu Vaccination Rate by State"))
    
    # COVID barplot
    fig.add_trace(
        go.Bar(x=df[state_col], y=df[covid_col], text=df[covid_col], textposition="outside",
               marker_color="skyblue", name="COVID"),
        row=1, col=1
    )
    
    # Flu barplot
    fig.add_trace(
        go.Bar(x=df[state_col], y=df[flu_col], text=df[flu_col], textposition="outside",
               marker_color="lightgreen", name="Flu"),
        row=2, col=1 #, textfont_size=12
    )
    
    # Update layout
    fig.update_layout(
        title_text="Vaccination Rates by State (COVID vs Flu)",
        title_x=0.5,
        height=800,
        width=700,
        showlegend=False
    )
    
    fig.show()


plot_vaccination_rates_plotly(vaccination_rate_per_state)


# COMMAND ----------

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

def plot_vaccination_rates_plotly(df, state_col="SG_UF_NOT", covid_col="rate_vaccinated_covid", flu_col="rate_vaccinated_gripe", sort_by="rate_vaccinated_covid", label_font_size=15):
    """
    Plot vaccination rates (COVID and Flu) by state using Plotly with vertical subplots.
    
    Args:
        df (pd.DataFrame): DataFrame containing state and vaccination rates.
        state_col (str): Column name for states.
        covid_col (str): Column name for COVID vaccination rate.
        flu_col (str): Column name for Flu vaccination rate.
        sort_by (str): Column to sort states by (default 'covid_rate').
        label_font_size (int): Font size for percentage labels on bars.
    """
    
    # Sort by chosen column
    df = df.sort_values(sort_by, ascending=False)
    
    # Create vertical subplots (stacked, not sharing x-axis so labels appear in both)
    fig = make_subplots(rows=2, cols=1, shared_xaxes=False,
                        subplot_titles=("COVID Vaccination Rate by State", "Flu Vaccination Rate by State"))
    
    # COVID barplot
    fig.add_trace(
        go.Bar(
            x=df[state_col],
            y=df[covid_col],
            text=[f"{v}%" for v in df[covid_col]],
            textposition="outside",
            textfont_size=label_font_size,
            marker_color="skyblue",
            name="COVID"
        ),
        row=1, col=1
    )
    
    # Flu barplot
    fig.add_trace(
        go.Bar(
            x=df[state_col],
            y=df[flu_col],
            text=[f"{v}%" for v in df[flu_col]],
            textposition="outside",
            textfont_size=label_font_size,
            marker_color="lightgreen",
            name="Flu"
        ),
        row=2, col=1
    )
    
    # Update layout
    fig.update_layout(
        title_text="Vaccination Rates by State (COVID vs Flu)",
        title_x=0.5,
        height=800,
        width=1200,
        showlegend=False,
        xaxis=dict(title="UF"),     # x-axis title for top subplot
        xaxis2=dict(title="UF")     # x-axis title for bottom subplot
    )
    
    fig.show()

# COMMAND ----------

plot_vaccination_rates_plotly(vaccination_rate_per_state, label_font_size=16)

# COMMAND ----------

profile_dataframe(srag_df.filter(F.year("DT_NOTIFIC") == 2025))

# COMMAND ----------

html_template =
