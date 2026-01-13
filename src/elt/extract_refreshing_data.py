# Databricks notebook source
!pip install uv
!uv sync --active --quiet
dbutils.library.restartPython()

# COMMAND ----------

import pprint
import json
import pandas as pd
import toml
import requests
import zipfile
import io
from datetime import datetime, timedelta
import sys

# COMMAND ----------

env_vars = toml.load("../../conf/env_vars.toml")

# COMMAND ----------

# MAGIC %md
# MAGIC # 2025 SRAG - Banco Vivo

# COMMAND ----------

# Find most recent SRAG files in S3 of OpenDataSUS

def find_latest_srag(years=[2025,2026], days_back=30, file_format=None):
    """
    Searches for the most recent SRAG file by testing retroactive dates.

    Args:
        years: Epidemiological years (e.g. 2025 and 2026)
        days_back: How many days back to test
        file_format: SRAG file format (e.g. csv)

    Returns:
        Dict with URLs found by format
    """
    if file_format is None:
        file_format = 'csv'
    found = {}
    for year in years:

        base_url = f"https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SRAG/{year}"
        year_suffix = str(year)[-2:]  # 2025 -> 25

        print(f"Buscando arquivos SRAG {year} mais recentes...")
        print(f"Testando últimos {days_back} dias...")
        print(f"\nAno: {year}")
        for i in range(days_back):
            date = datetime.now() - timedelta(days=i)
            date_str = date.strftime("%d-%m-%Y")
        
            filename = f"INFLUD{year_suffix}-{date_str}.{file_format}"
            url = f"{base_url}/{filename}"
        
            try:
                resp = requests.head(url, timeout=5)
                if resp.status_code == 200:
                    print(f"  ✅ Encontrado: {filename}")
                    found[year] = {
                        'url': url,
                        'date': date_str,
                        'filename': filename
                    }
                    break
                else:
                    print(".", end="", flush=True)
            except requests.exceptions.RequestException:
                print("x", end="", flush=True)
    
        if year not in found:
            print(f"\n  ❌ Nenhum arquivo {year} encontrado")
    return found

# COMMAND ----------

# Create table to store last SRAG database updates for 2025 and 2026
SRAG_UPDATE_TABLE = f"{env_vars['CATALOG']}.{env_vars['SCHEMA']}.srag_data_update"
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SRAG_UPDATE_TABLE} (
    year INT,
    latest_srag_update DATE
)
""")

def update_srag_last_date(year: int, new_date: str):
    """
    Updates the SRAG last update date for a given year.
    If the date is different from the current one, appends a new row.
    """
    from pyspark.sql.functions import col

    df = spark.table(SRAG_UPDATE_TABLE).filter(col("year") == year)
    if df.count() == 0 or df.select("latest_srag_update").first()[0] != new_date:
        spark.sql(f"""
            INSERT INTO dev.dev_maria.srag_data_update (year, latest_srag_update)
            VALUES ({year}, '{new_date}')
        """)

def get_srag_last_date(year: int):
    """
    Returns the last SRAG update date for a given year.
    """
    from pyspark.sql.functions import col

    df = spark.table(SRAG_UPDATE_TABLE).filter(col("year") == year)
    if df.count() == 0:
        return None
    return df.orderBy(col("latest_srag_update").desc()).select("latest_srag_update").first()[0]

# COMMAND ----------

# Search for the most recent SRAG file
results = find_latest_srag(days_back=30
)

# COMMAND ----------

# Iterate over the results found, if a new update is avaiable, load the new data and update the feature store.
for year in results.keys():
    latest_data_upload  = get_srag_last_date(year)
    new_update = pd.to_datetime(results[year]["date"]).date()
    if (latest_data_upload is None) or (latest_data_upload < new_update):
        print(f"A new update in the {year} dataset is available. Loading the dataset updated in {new_update}...")
        update_srag_last_date(year, new_update)

        latest_srag_update_url = results[year]["url"]
        corrected_srag_url = "s3a:/" + latest_srag_update_url.split("amazonaws.com")[1]
        srag_table_name = F'{env_vars["CATALOG"]}.{env_vars["SCHEMA"]}.srag_vigilance'
        srag_table_new = F'{env_vars["CATALOG"]}.{env_vars["SCHEMA"]}.srag_2025'
        srag_schema = spark.read.table(srag_table_name).schema

        df_influenza_new = spark.read.options(delimiter=";", header=True).schema(srag_schema).csv(corrected_srag_url, dateFormat="dd/MM/yyyy")

        # Save the new data to the SRAG table.
        df_influenza_new.write.mode("overwrite").saveAsTable(srag_table_new)
        print("df_influenza_new: num_rows = ", df_influenza_new.count(), ", num_cols = ", len(df_influenza_new.columns))

        srag_table = spark.read.table(srag_table_name)
        row_count_before, col_count_before = srag_table.count(), len(srag_table.columns)
        print(srag_table_name, ": num_rows = ", row_count_before, ", num_cols = ", col_count_before)

        # Append new data to srag table.
        spark.sql(f"""MERGE INTO {srag_table_name}
        USING {srag_table_new}
        ON {srag_table_name}.NU_NOTIFIC = {srag_table_new}.NU_NOTIFIC
        WHEN MATCHED THEN
        UPDATE SET *
        WHEN NOT MATCHED THEN
        INSERT *
        """)

        srag_table_after = spark.read.table(srag_table_name)
        print(srag_table_name, "after merging new data: num_rows = ", srag_table_after.count(), ", num_cols = ", len(srag_table_after.columns))
        print("Number of new rows appended = ", srag_table_after.count() - srag_table.count())

# COMMAND ----------

# MAGIC %md
# MAGIC # 2025 Hospital UTI

# COMMAND ----------

# HOSPITAL package id.
hospital_package_id = "791730b2-50bd-41ba-adf2-d915b88f712a"
latest_hospital_update_url = "https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/Leitos_SUS/Leitos_csv_2025.zip"
latest_hospital_update_url

# COMMAND ----------

hospital_table_name = F'{env_vars["CATALOG"]}.{env_vars["SCHEMA"]}.hospital'
hospital_table_name_2025 = F'{env_vars["CATALOG"]}.{env_vars["SCHEMA"]}.hospital_2025'
hospital_schema = spark.read.table(hospital_table_name).schema
hospital_columns = spark.read.table(hospital_table_name).columns

# COMMAND ----------

# Download zip file into memory.
r = requests.get(latest_hospital_update_url)
z = zipfile.ZipFile(io.BytesIO(r.content))

# Pick the first CSV name of zip file.
csv_filename = z.namelist()[0]

# Load into Pandas directly from the zip with the correct encoding
with z.open(csv_filename) as hospital_file:
    pd_df_hospital = pd.read_csv(
        hospital_file, 
        delimiter=";", 
        encoding='latin1', 
        on_bad_lines='skip'
    )[hospital_columns]

# Save the new hospital data into a. spark dataframe.
df_hospital_2025 = spark.createDataFrame(pd_df_hospital, schema=hospital_schema)

# COMMAND ----------

pd_df_hospital.head(2)

# COMMAND ----------

# Check column's match before merging the data.

cols_df_hospital_uti_all = set(hospital_columns)
cols_df_hospital_uti_2025 = set(df_hospital_2025.columns)

print("Only in df_hospital_uti_all:", cols_df_hospital_uti_2025 - cols_df_hospital_uti_all)
print("Only in df_hospital_uti_all:", cols_df_hospital_uti_all - cols_df_hospital_uti_2025)
print("In all:", len(cols_df_hospital_uti_all & cols_df_hospital_uti_2025))

# COMMAND ----------

# Save the new data into a table.
df_hospital_2025.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(hospital_table_name_2025)

# COMMAND ----------

hospital_table_2025 = spark.read.table(hospital_table_name_2025)
print("num_rows = ", hospital_table_2025.count())
print("num_cols = ", len(hospital_table_2025.columns))

# COMMAND ----------

hospital_table = spark.read.table(hospital_table_name)
row_count_before, col_count_before = hospital_table.count(), len(hospital_table.columns)
print(hospital_table_name, ": num_rows = ", row_count_before, ", num_cols = ", col_count_before)

# Append 2025 new data to hospital table.
spark.sql(f"""
MERGE INTO {hospital_table_name} AS target
USING {hospital_table_name_2025} AS source
ON target.CNES = source.CNES
AND target.COMP = source.COMP
WHEN MATCHED THEN
  UPDATE SET *
WHEN NOT MATCHED THEN
  INSERT *
""")

hospital_table_after = spark.read.table(hospital_table_name)
print(hospital_table_name, "after merging new data: num_rows = ", hospital_table_after.count(), ", num_cols = ", len(hospital_table_after.columns))
print("Number of new rows appended = ", hospital_table_after.count() - row_count_before)
