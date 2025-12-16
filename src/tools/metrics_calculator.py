import os
import pandas as pd
import json
import toml
from typing import Optional
from pyspark.sql.connect.dataframe import DataFrame
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import json
import requests
import pyspark.sql.types as T
from plotly.subplots import make_subplots
from datetime import timedelta, date
from pydantic import BaseModel, Field
from typing import Optional
from datetime import date
import os
from langchain.tools import BaseTool
from typing import Type
from jinja2 import Template


class SRAGMetrics:
    def __init__(
        self, 
        srag_df: Optional[DataFrame] = None, 
        hospital_df: Optional[DataFrame] = None):
        """
        srag_df: optional main dataset (e.g., monthly cases)
        hospital_df: optional secondary dataset (e.g., hospitalizations)
        """
        spark = SparkSession.builder.getOrCreate()
        catalog = os.environ["CATALOG"]
        schema = os.environ["FS_SCHEMA"]
        self.srag_df = srag_df if srag_df is not None else spark.read.table(f'{catalog}.{schema}.srag_features')
        self.hospital_df = hospital_df if hospital_df is not None else spark.read.table(f'{catalog}.{schema}.hospital_features')

    # Metric functions
    def calculate_cases_per_month(
        self,
        srag_df: Optional[DataFrame] = None,
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
        if srag_df is None:
            srag_df = self.srag_df
        # else:
        #     spark = SparkSession.builder.getOrCreate()
        #     catalog = os.environ["CATALOG"]
        #     schema = os.environ["FS_SCHEMA"]
        #     self.srag_df = srag_df if srag_df is not None else spark.read.table(f'{catalog}.{schema}.srag_features')

        # Default period = last 12 months.
        if end_date is None:
            end_date = pd.to_datetime("today").strftime("%Y-%m-%d")
        if start_date is None:
            start_date = (pd.to_datetime(end_date) - pd.DateOffset(months=12)).strftime("%Y-%m-%d")
        # Filter Spark DataFrame.
        df_filtered = srag_df.filter((F.col("DT_NOTIFIC") >= F.lit(start_date)) & 
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
            increase_rate (float): increase rate of cases compared to the previous period.
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
        srag_df: Optional[DataFrame] = None,
        start_date: Optional[str] = None, 
        end_date: Optional[str] = None,
        ) -> pd. DataFrame :
        """
        Calculates number of cases per day.
        
        Args:
            df (DataFrame): Spark DataFrame with column DT_NOTIFIC (date) of the period of time to calculate the daily number of SRAG cases.
            start_date (str): Start date in 'yyyy-MM-dd'. If None, defaults to 30 days interval from last day with available data.
            end_date (str): End date in 'yyyy-MM-dd'. If None, defaults to today.
            
        Returns:
            Pandas DataFrame with ['DT_NOTIFIC', 'count'].
        """
        if srag_df is None:
            srag_df = self.srag_df
        # Default period = last 30 days
        if end_date is None:
            end_date = srag_df.select(F.max("DT_NOTIFIC")).collect()[0][0]
        if start_date is None:
            start_date = (pd.to_datetime(end_date) - pd.DateOffset(days=30)).strftime("%Y-%m-%d")

        # Filter Spark DataFrame to the period of time desired.
        filtered = srag_df.filter((F.col("DT_NOTIFIC") >= F.lit(start_date)) & 
                            (F.col("DT_NOTIFIC") <= F.lit(end_date)))
        
        # Aggregate cases per day.
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
    
    def calculate_icu_admission_rate(self, srag_df: Optional[pd.DataFrame] = None, start_date=None, end_date=None):
        """
        Calculates the intensive care unit (ICU) admission rate relative to the number of SRAG cases per month
        over the selected period. Defaults to the last month with available data.
        
        Args:
            srag_df (DataFrame): Spark DataFrame with columns DT_NOTIFIC, DT_ENTUTI, DT_SAIDUTI.
            start_date (str): Start date (yyyy-MM-dd).
            end_date (str): End date (yyyy-MM-dd).
            
        Returns:
            Pandas DataFrame with columns [year_month, cases, admitted, admission_rate].
        """
        if srag_df is None:
            srag_df = self.srag_df
        if end_date is None:
            end_date = srag_df.select(F.max("DT_NOTIFIC")).collect()[0][0]
        if start_date is None:
            start_date = end_date.replace(day=1)
        
        # Filter by the selected period
        df_filtered = srag_df.filter(
            (F.col("DT_NOTIFIC") >= F.lit(start_date)) & 
            (F.col("DT_NOTIFIC") <= F.lit(end_date))
            )
        
        # Cases per month (based on DT_NOTIFIC)
        cases = (
            df_filtered
            .withColumn("year_month", F.date_format("DT_NOTIFIC", "yyyy-MM"))
            .groupBy("year_month")
            .agg(F.count("*").alias("cases"))
        )
        
        # ICU admissions per month
        # Create monthly intervals
        months = pd.date_range(start=start_date, end=end_date, freq="MS")  # MS = month start
        
        admitted_list = []
        for m in months:
            m_start = pd.to_datetime(m).strftime("%Y-%m-%d")
            m_end = (pd.to_datetime(m) + pd.offsets.MonthEnd(0)).strftime("%Y-%m-%d")
            
            # Condition: admitted in the month (ICU entry or exit within the month, 
            # or ICU entry before the month start and exit after month end)
            cond = (
                ((F.col("DT_ENTUTI") >= F.lit(m_start)) & (F.col("DT_ENTUTI") <= F.lit(m_end))) |
                ((F.col("DT_SAIDUTI") >= F.lit(m_start)) & (F.col("DT_SAIDUTI") <= F.lit(m_end))) |
                ((F.col("DT_ENTUTI") <= F.lit(m_start)) & (F.col("DT_SAIDUTI") >= F.lit(m_end)))
            )
            
            count_admitted = srag_df.filter(cond).count()
            admitted_list.append({"year_month": m.strftime("%Y-%m"), "admitted_icu": count_admitted})
        
        admitted_pd = pd.DataFrame(admitted_list)
        
        # Merge cases + ICU admissions
        result = cases.toPandas().merge(admitted_pd, on="year_month", how="outer").fillna(0)
        
        # Calculate admission rate
        result["icu_admission_rate"] = round((result["admitted_icu"] / result["cases"].replace(0, pd.NA)) * 100, 2)
        
        return result.sort_values("year_month")
        
    def calculate_icu_occupancy_per_state(self, srag_df: Optional[pd.DataFrame] = None, hospital_df: Optional[pd.DataFrame] = None):
        """
        Calculates intensive care unit (ICU) bed occupancy rate of the current month (or month of last data available)).
        
        Args:
            srag_df (pd.DataFrame): DataFrame with srag df notification data.
            hospital_df (pd.DataFrame): DataFrame with bed counts per category (adulto, pediatrica, neonatal)
                                    + keys ['month_year', 'SG_UF_NOT'].
                                    
        Returns:
            pd.DataFrame with occupancy rates per category and overall.
        """
        if srag_df is None:
            srag_df = self.srag_df
        if hospital_df is None:
            hospital_df = self.hospital_df

        spark = SparkSession.builder.getOrCreate()

        # Calculate icu bead occupancy rate per state and category.
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
        # Calculate number of patients were admitted to icu in the last month or admitted in this year but did not leave yeat.
        cond = (
        ((F.col("DT_ENTUTI") >= F.lit(start_date)) & (F.col("DT_ENTUTI") <= F.lit(last_date))) |
        ((F.col("DT_SAIDUTI") >= F.lit(start_date)) & (F.col("DT_SAIDUTI") <= F.lit(last_date))) |
        ((F.col("DT_ENTUTI") <= F.lit(start_date)) & (F.col("DT_SAIDUTI") >= F.lit(last_date)))
        )
        state_icu_bed_sum = (
            srag_filtered_1.filter(cond).groupBy("SG_UF_NOT")
            .pivot("classificacao_etaria_leito")
            .count())   
        patients_df = patients_df.unionByName(state_icu_bed_sum, allowMissingColumns=True)
        patients_df = patients_df.toPandas().fillna(0).rename(columns={"SG_UF_NOT": "uf"}) 

        # Filter icu beds available in the last month.
        # Get max month_year
        max = hospital_df.select(F.max("month_year")).collect()[0][0]
        hospital_df_filtered  = hospital_df.filter(
            F.col("month_year") == max).withColumnRenamed("UF", "uf").toPandas()
        hospital_df_filtered = hospital_df_filtered[["uf", "adulto", "pediatrica", "neonatal"]]

        # Merge patient_df (icu ocupancy) with hospital_df_filtered (icu bed availables per state).
        merged = patients_df.merge(
            hospital_df_filtered,
            on="uf",
            suffixes=("_patients", "_beds")
        )

        # Calculate rates per category
        for icu_cat in ["adulto", "pediatrica", "neonatal"]:
            merged[f"{icu_cat}_rate"] = (
                merged[f"{icu_cat}_patients"] / merged[f"{icu_cat}_beds"] * 100
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

        icu_ocuppancy_per_state_df = merged[
            ["month_year","uf", "adulto_rate", "pediatrica_rate", "neonatal_rate", "total_rate"]
            ].rename(columns={"month_year": "Mês/Ano", "uf": "UF", "adulto_rate": "Taxa de ocupação UTI adulto", "pediatrica_rate": "Taxa de ocupação UTI pediátrica", "neonatal_rate": "Taxa de ocupação UTI neonatal", "total_rate": "Taxa de ocupação total UTIs"})
        icu_ocuppancy_per_state_df[['Taxa de ocupação UTI adulto',
            'Taxa de ocupação UTI pediátrica', 'Taxa de ocupação UTI neonatal',
            'Taxa de ocupação total UTIs']] = icu_ocuppancy_per_state_df[['Taxa de ocupação UTI adulto',
            'Taxa de ocupação UTI pediátrica', 'Taxa de ocupação UTI neonatal',
            'Taxa de ocupação total UTIs']].clip(upper=100)

        icu_ocuppancy_per_state_df = icu_ocuppancy_per_state_df.sort_values(by="UF")
        return icu_ocuppancy_per_state_df

    def calculate_vaccination_rate(self, srag_df:Optional[DataFrame]=None, year_month:str=None, group_by_state:bool=False):
        """
        Calculate the percentage of vaccinated patients over the number of SRAG notifications by month.
        
        Args:
            srag_df (DataFrame): Input Spark DataFrame with columns VACINA_COV, NU_NOTIFIC, DT_NOTIFIC, UF
            group_by_state (bool): If True, group results by state.
            year_month (str): Optional year-month string in format "yyyy-MM". If None, defaults to the last month with available data.
        
        Returns:
            DataFrame: Spark DataFrame with month (and state if selected), total_notifications,
                    vaccinated, percentage_vaccinated
        """
        if srag_df is None:
            srag_df = self.srag_df
        if year_month is not None:
            year = int(year_month[:4])
            month = int(year_month[5:])
        else:
            last_date = srag_df.select(F.max("DT_NOTIFIC")).collect()[0][0]
            year = last_date.year
            month = last_date.month

        srag_df = srag_df.filter((F.year("DT_NOTIFIC") == year) & (F.month("DT_NOTIFIC") == month))
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
        
        total_df = total_df.toPandas().rename(columns={"year_month": "Mês/Ano", "SG_UF_NOT": "UF", "total_notifications": "Total de Notificações", "vaccinated_covid": "Número de Pacientes Vacinados COVID", "vaccinated_gripe": "Número de Pacientes Vacinados Gripe", "rate_vaccinated_covid": "Taxa de vacinados COVID (%)", "rate_vaccinated_gripe": "Taxa de vacinados Gripe (%)"})   
        return total_df
    
    # Agent-facing run method
    def generate_report_metrics(self, query: str = None) -> dict:
        """Run function to calculate the metrics and return the results as a JSON string.
        
        last_srag_notification_available: Max date of srag notifications (last data ingested from Open DataSUS), in format "dd-mm-yyyy"
        report_date: Current date.
        cases_last_seven_days: Number of cases of SRAG notified in the last seven days.
        total_cases_month: Number of cases in the last month.
        icu_ocuppancy_per_state_table: HTML table with ICU occupancy rate per category (adult, pediatric and neonatal) per state, regarding the last month.
        last_month_cases_variation_rate: Percentage variation of cases in the last month
        last_month_year: Year-month of the last month
        icu_admission_count: Number of patients into the ICU in the last month.
        icu_admission_rate: Percentage of patients into the ICU in the last month.
        """
        metric_results = {
            "last_srag_notification_available": self.calculate_cases_per_day()["DT_NOTIFIC"].max().strftime("%d-%m-%Y"),
            "report_date": date.today(),
            "cases_last_seven_days": self.calculate_cases_per_day().iloc[-7:]["count"].sum(),
            "total_cases_month": self.calculate_cases_per_month()["count"][-1:].iloc[0],
            "icu_ocuppancy_per_state_table": self.calculate_icu_occupancy_per_state().to_html(classes="icu-table", index=False),
            "last_month_cases_variation_rate": round(self.calculate_cases_per_month_variation_rate(), 2),
            "last_month_year": self.calculate_icu_admission_rate()["year_month"].iloc[0],
            "icu_admission_count": self.calculate_icu_admission_rate()["admitted_icu"].iloc[0],
            "icu_admission_rate": self.calculate_icu_admission_rate()["icu_admission_rate"].iloc[0],
        }
        return metric_results
    

# Define the schema for the tool output
class SRAGMetricsOutput(BaseModel):
    last_srag_notification_available: str = Field(..., description="Max date of SRAG notifications, format dd-mm-yyyy")
    report_date: date = Field(..., description="Current report date")
    cases_last_seven_days: int = Field(..., description="Number of SRAG cases in the last 7 days")
    total_cases_month: int = Field(..., description="Number of SRAG cases in the last full month")
    icu_ocuppancy_per_state_table: str = Field(..., description="ICU occupancy table in HTML format")
    last_month_cases_variation_rate: float = Field(..., description="Variation rate of cases in the last month")
    last_month_year: str = Field(..., description="Year-month of last month in format YYYY-MM")
    icu_admission_count: int = Field(..., description="ICU admission count for last month")
    icu_admission_rate: float = Field(..., description="ICU admission rate for last month")